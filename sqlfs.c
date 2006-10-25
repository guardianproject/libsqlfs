/******************************************************************************
Copyright 2006 Palmsource, Inc (an ACCESS company). 
 
This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.
 
This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.
 
You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 
*****************************************************************************/
/*!
 * @file sqlfs.c
 *
 * @brief file system on top of a SQL database library
 *  API implementation
 *
 *****************************************************************************/

/* the file system is stored in a SQLite table, with the following columns
 
 
full key path      type     inode      uid      gid        mode   acl      attributes         atime  mtime ctime size  block_size
(text)            (text)    (integer) (integer) (integer)   (integer)   (text)    (text)        (integer) ...
 
the key path must start with "/" and is case sensitive
 
the type can be one of these:  "int", "double",  "string", "dir", "sym link" and "blob"
 
 
for Blobs we will divide them into 8k pieces, each occupying an BLOB object in database indexed by a block number 
which starts from 0
 
created by
 
 CREATE TABLE meta_data(key text, type text, inode integer, uid integer, gid integer, mode integer,  acl text, attribute text,
    atime integer, mtime integer, ctime integer, size integer, block_size integer, primary key (key), unique(key)) ;
    
 CREATE TABLE value_data (key text, block_no integer, data_block blob, unique(key, block_no));   
 
 create index meta_index on meta_data (key);
 create index value_index on value_data (key, block_no);
 
 
*/

/* currently permission control due to the current directory not implemented */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <time.h>
#include "sqlfs.h"
#include "sqlite3.h"



#define INDEX 0

#define PREPARE_STMT\
    stmt = get_sqlfs(sqlfs)->stmts[INDEX];\
    r = SQLITE_OK; \
    if (stmt)\
    {\
        if (sqlite3_expired(stmt))\
        {\
            sqlite3_finalize(stmt);\
            r = ~SQLITE_OK;\
        }\
    }\
    else r = ~SQLITE_OK;\
    if (r != SQLITE_OK)

#define DONE_PREPARE  if (r == SQLITE_OK) get_sqlfs(sqlfs)->stmts[INDEX] = stmt; else get_sqlfs(sqlfs)->stmts[INDEX] = 0;

#define SQLITE3_PREPARE(a, b, c, d, e)\
    PREPARE_STMT \
    r = sqlite3_prepare((a), (b), (c), (d), (e));\
    DONE_PREPARE


static const int BLOCK_SIZE = 128 * 1024;

static pthread_key_t sql_key;

static char default_db_file[PATH_MAX] = { 0 };


static int max_inode = 0;

static void * sqlfs_t_init(const char *);
static void sqlfs_t_finalize(void *arg);

static void delay(int ms)
{
    struct timeval timeout;
    timeout.tv_sec = ms / 1000 ;
    timeout.tv_usec = 1000 * ( ms % 1000 );

    select(0, 0, 0, 0, &timeout);
}

static __inline__ int sql_step(sqlite3_stmt *stmt)
{
    int r, i;
    for (i = 0; i < (1 * 1000 / 100); i++)
    {
        r = sqlite3_step(stmt);
        if (r != SQLITE_BUSY)
            break;
        delay(100);
    }

    return r;

}

static __inline__ sqlfs_t *get_sqlfs(sqlfs_t *p)
{
    sqlfs_t *sqlfs;

    if (p)
        return p;

    sqlfs = (sqlfs_t *) (pthread_getspecific(sql_key));
    if (sqlfs)
        return sqlfs;

    sqlfs =  (sqlfs_t*) sqlfs_t_init(default_db_file);
    pthread_setspecific(sql_key, sqlfs);
    return sqlfs;
}
static __inline__ int get_new_inode(void)
{
    return ++max_inode;
}

static __inline__ void remove_tail_slash(char *str)
{
    char *s = str + strlen(str) - 1;
    while (s != str - 1)
    {
        if (*s != '/')
            break;
        *s = 0;
        s--;
    }
}

static __inline__ char *make_str_copy(const char *str)
{
    if (str == 0)
        return 0;
    return strdup(str);
}

static void show_msg(FILE *f, char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);

    vfprintf(f, fmt, ap);
    va_end(ap);
}



void clean_attr(key_attr *attr)
{
    if (attr->path)
        free(attr->path);
    if (attr->type)
        free(attr->type);
    memset(attr, 0, sizeof(*attr));
}


void clean_value(key_value *value)
{
    if (value->data)
        free(value->data);
    memset(value, 0, sizeof(*value));
}

/*static pthread_mutex_t transaction_lock = PTHREAD_MUTEX_INITIALIZER;
*/
#define TRANS_LOCK //pthread_mutex_lock(&transaction_lock);
#define TRANS_UNLOCK //pthread_mutex_unlock(&transaction_lock);


#undef INDEX
#define INDEX 100

static int begin_transaction(sqlfs_t *sqlfs)
{
    int i;
#ifdef FUSE
    const char *cmd = "begin exclusive;";

#else
    const char *cmd = "begin;";
#endif

    sqlite3_stmt *stmt;
    const char *tail;
    int r = SQLITE_OK;
    TRANS_LOCK


    if (get_sqlfs(sqlfs)->transaction_level == 0)
    {
        /*assert(sqlite3_get_autocommit(get_sqlfs(sqlfs)->db) != 0);*/

        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1,  &stmt,  &tail);
        for (i = 0; i < 10; i++)
        {
            r = sqlite3_step(stmt);
            if (r != SQLITE_BUSY)
                break;
            delay(100);
        }
        sqlite3_reset(stmt);
        if (r == SQLITE_DONE)
            r = SQLITE_OK;
        if (r == SQLITE_BUSY)
        {
            TRANS_UNLOCK;
            show_msg(stderr, "database is busy!\n");
            return r;  /* busy, return back */
        }
        get_sqlfs(sqlfs)->in_transaction = 1;
    }
    get_sqlfs(sqlfs)->transaction_level++;
    TRANS_UNLOCK
    return r;
}

#undef INDEX
#define INDEX 101


static int commit_transaction(sqlfs_t *sqlfs, int r0)
{
    int i;
    /* commit if r0 is 1
       rollback if r0 is 0
    */
    static const char *cmd1 = "commit;", *cmd2 = "rollback;";

    int r = SQLITE_OK;
    sqlite3_stmt *stmt, *stmt1, *stmt2;
    const char *tail;

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1,  &stmt,  &tail);

    stmt1 = stmt;

#undef INDEX
#define INDEX 102

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1,  &stmt,  &tail);
    stmt2 = stmt;

    if (r0 != 0)
        stmt = stmt1;
    else
        stmt = stmt2;
    TRANS_LOCK


    /*assert(get_sqlfs(sqlfs)->transaction_level > 0);*/

    /*assert(get_sqlfs(sqlfs)->transaction_level >= 0);*/
    if ((get_sqlfs(sqlfs)->transaction_level - 1 == 0) && (get_sqlfs(sqlfs)->in_transaction))
    {
        for (i = 0; i < 10; i++)
        {
            r = sqlite3_step(stmt);
            if (r != SQLITE_BUSY)
                break;
            delay(100);
        }
        sqlite3_reset(stmt);
        if (r == SQLITE_DONE)
            r = SQLITE_OK;
        if (r == SQLITE_BUSY)
        {
            TRANS_UNLOCK;
            show_msg(stderr, "database is busy!\n");
            return r;  /* busy, return back */
        }
        //**assert(sqlite3_get_autocommit(get_sqlfs(sqlfs)->db) != 0);*/
        get_sqlfs(sqlfs)->in_transaction = 0;
    }
    get_sqlfs(sqlfs)->transaction_level--;

    /*if (get_sqlfs(sqlfs)->transaction_level == 0)
        assert(get_sqlfs(sqlfs)->in_transaction == 0);*/
    TRANS_UNLOCK
    return r;
}

#undef INDEX
#define INDEX 103

static int break_transaction(sqlfs_t *sqlfs, int r0)
{
    int i;
    /* commit if r0 is 1
       rollback if r0 is 0
    */
    static const char *cmd1 = "commit;", *cmd2 = "rollback;";

    int r = SQLITE_OK;
    sqlite3_stmt *stmt, *stmt1, *stmt2;
    const char *tail;

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1,  &stmt,  &tail);

    stmt1 = stmt;

#undef INDEX
#define INDEX 104

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1,  &stmt,  &tail);
    stmt2 = stmt;


    if (r0 != 0)
        stmt = stmt1;
    else
        stmt = stmt2;

    TRANS_LOCK

    if (get_sqlfs(sqlfs)->in_transaction)
    {
        for (i = 0; i < 10; i++)
        {
            r = sqlite3_step(stmt);
            if (r != SQLITE_BUSY)
                break;
            delay(100);
        }
        sqlite3_reset(stmt);
        if (r == SQLITE_DONE)
            r = SQLITE_OK;
        if (r == SQLITE_BUSY)
        {
            TRANS_UNLOCK;
            show_msg(stderr, "database is busy!\n");
            return r;  /* busy, return back */
        }
        //**assert(sqlite3_get_autocommit(get_sqlfs(sqlfs)->db) != 0);*/
        get_sqlfs(sqlfs)->in_transaction = 0;
    }
    TRANS_UNLOCK
    return r;
}



/*#ifndef FUSE
#define BEGIN
#define COMPLETE(r)
#else
*/
#define BEGIN begin_transaction(get_sqlfs(sqlfs));
#define COMPLETE(r) commit_transaction(get_sqlfs(sqlfs), (r));
/*#endif*/


#undef INDEX
#define INDEX 1


static __inline__ int get_current_max_inode(sqlfs_t *sqlfs)
{
    sqlite3_stmt *stmt;
    const char *tail;
    static const char *cmd = "select max(inode) from meta_data;";
    int r, result = 0;

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1,  &stmt,  &tail);


    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return 0;
    }


    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    else
        result = sqlite3_column_int(stmt, 0);
    sqlite3_reset(stmt);
    return result;

}


#undef INDEX
#define INDEX 2


static int key_exists(sqlfs_t *sqlfs, const char *key, size_t *size)
{
    sqlite3_stmt *stmt;
    const char *tail;
    static const char *cmd = "select size from meta_data where key = :key;";
    int r, result = 0;
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1,  &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return 0;
    }


    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        if (r == SQLITE_BUSY)
            result = 2;
    }

    else
    {
        if (size)
            *size = sqlite3_column_int64(stmt, 0);
        result = 1;
    }
    sqlite3_reset(stmt);
    return result;


}

#undef INDEX
#define INDEX 3


static int key_is_dir(sqlfs_t *sqlfs, const char *key)
{
    sqlite3_stmt *stmt;
    const char *tail, *t;
    static const char *cmd = "select type from meta_data where key = :key;";
    int r, result = 0;
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1,  &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return r;
    }

    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        if (r == SQLITE_BUSY)
            result = 2;
    }

    else
    {
        t = sqlite3_column_text(stmt, 0);

        if (t && !strcmp(TYPE_DIR, t))
            result = 1;

    }
    sqlite3_reset(stmt);
    return result;


}


#undef INDEX
#define INDEX 4

static int key_accessed(sqlfs_t *sqlfs, const char *key)
{
    sqlite3_stmt *stmt;
    const char *tail;
    static const char *cmd = "update meta_data set atime = :atime where key = :key;";
    int r;
    time_t now;

    time(&now);
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1,  &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return r;
    }

    r = sqlite3_bind_int64(stmt, 1, now);
    r = sqlite3_bind_text(stmt, 2, key, -1, SQLITE_STATIC);
    r = sqlite3_step(stmt);
    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    else if (r == SQLITE_BUSY)
        ;
    else
        r = SQLITE_OK;
    sqlite3_reset(stmt);
    return r;
}


#undef INDEX
#define INDEX 5

static int key_modified(sqlfs_t *sqlfs, const char *key)
{
    sqlite3_stmt *stmt;
    const char *tail;
    time_t now ;
    static const char *cmd = "update meta_data set atime = :atime, mtime = :mtime, ctime = :ctime where key = :key;";
    int r;
    time(&now);
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1,  &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return r;
    }

    sqlite3_bind_int64(stmt, 1, now);
    sqlite3_bind_int64(stmt, 2, now);
    sqlite3_bind_int64(stmt, 3, now);
    sqlite3_bind_text(stmt, 4, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    else
        r = SQLITE_OK;
    sqlite3_reset(stmt);
    return r;
}


#undef INDEX
#define INDEX 6



static int remove_key(sqlfs_t *sqlfs, const char *key)
{
    int r;
    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd1 = "delete from meta_data where key = :key;";
    static const char *cmd2 = "delete from value_data where key = :key;" ;
    BEGIN
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt, &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
    }
    else
    {
        r = SQLITE_OK;
    }
    sqlite3_reset(stmt);


#undef INDEX
#define INDEX 7

    if (r == SQLITE_OK)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt, &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            COMPLETE(1)
            return r;
        }
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
        r = sql_step(stmt);
        if (r != SQLITE_DONE)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        }
        else
        {
            r = SQLITE_OK;
        }
        sqlite3_reset(stmt);
    }
    COMPLETE(1)
    return r;
}



#undef INDEX
#define INDEX 8



static int remove_key_subtree(sqlfs_t *sqlfs, const char *key)
{
    int r;
    const char *tail;
    sqlite3_stmt *stmt;
    char pattern[PATH_MAX];
    static const char *cmd1 = "delete from meta_data where key glob :pattern;";
    static const char *cmd2 = "delete from value_data where key glob :pattern;" ;
    char *lpath;

    lpath = strdup(key);
    remove_tail_slash(lpath);
    sprintf(pattern, "%s/*", lpath);
    free(lpath);
    BEGIN
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt, &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, pattern, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
    }
    else
    {
        r = SQLITE_OK;
    }
    sqlite3_reset(stmt);


#undef INDEX
#define INDEX 9

    if (r == SQLITE_OK)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt, &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            COMPLETE(1)
            return r;
        }
        sqlite3_bind_text(stmt, 1, pattern, -1, SQLITE_STATIC);
        r = sql_step(stmt);
        if (r != SQLITE_DONE)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        }

        else
        {
            r = SQLITE_OK;
        }
        sqlite3_reset(stmt);
    }
    if (r == SQLITE_OK)
    {
        r = remove_key(sqlfs, key);
    }
    COMPLETE(1)
    return r;
}


#undef INDEX
#define INDEX 10


static int remove_key_subtree_with_exclusion(sqlfs_t *sqlfs, const char *key, const char *exclusion_pattern)
{
    int r;
    const char *tail;
    sqlite3_stmt *stmt;
    char pattern[PATH_MAX];
    char n_pattern[PATH_MAX];
    static const char *cmd1 = "delete from meta_data where (key glob :pattern) and not (key glob :n_pattern) ;";
    static const char *cmd2 = "delete from value_data where (key glob :pattern) and not (key glob :n_pattern) ;" ;
    static const char *cmd3 = "select key from meta_data where (key glob :n_pattern) ;" ;
    char *lpath;

    lpath = strdup(key);
    remove_tail_slash(lpath);
    snprintf(pattern, sizeof(pattern), "%s/*", lpath);

    snprintf(n_pattern, sizeof(n_pattern), "%s/%s", lpath, exclusion_pattern);
    free(lpath);
    BEGIN
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt, &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, pattern, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, n_pattern, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
    }
    else
    {
        r = SQLITE_OK;
    }
    sqlite3_reset(stmt);

#undef INDEX
#define INDEX 11

    if (r == SQLITE_OK)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt, &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            COMPLETE(1)
            return r;
        }
        sqlite3_bind_text(stmt, 1, pattern, -1, SQLITE_STATIC);
        r = sql_step(stmt);
        if (r != SQLITE_DONE)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        }
        else
        {
            r = SQLITE_OK;
        }
        sqlite3_reset(stmt);
    }


#undef INDEX
#define INDEX 12

    if (r == SQLITE_OK)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd3, -1, &stmt, &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            COMPLETE(1)
            return r;
        }
        sqlite3_bind_text(stmt, 1, n_pattern, -1, SQLITE_STATIC);
        r = sql_step(stmt);
        if (r != SQLITE_ROW)
        {
            if (r == SQLITE_BUSY)
                ;
            else
                if (r != SQLITE_DONE)
                    show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

                else
                    r = SQLITE_NOTFOUND;
        }

        sqlite3_reset(stmt);

        if (r == SQLITE_NOTFOUND)
            r = remove_key(sqlfs, key);
        else if (r == SQLITE_BUSY)
            ;
        else
            r = SQLITE_OK;
    }
    COMPLETE(1)
    return r;
}



#undef INDEX
#define INDEX 13

static int rename_key(sqlfs_t *sqlfs, const char *old, const char *new)
{
    int r;
    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd1 = "update meta_data set key = :new where key = :old; ";
    static const char *cmd2 = "update value_data set key = :new where key = :old; ";
    BEGIN
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, new, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, old, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
    }
    else
    {
        r = SQLITE_OK;
    }
    sqlite3_reset(stmt);



#undef INDEX
#define INDEX 14


    if (r == SQLITE_OK)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt,  &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            COMPLETE(1)
            return r;
        }
        sqlite3_bind_text(stmt, 1, new, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, old, -1, SQLITE_STATIC);
        r = sql_step(stmt);
        if (r != SQLITE_DONE)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        }
        else
        {
            r = SQLITE_OK;
        }
        sqlite3_reset(stmt);
    }
    COMPLETE(1)
    return r;


}


#undef INDEX
#define INDEX 15


static int get_dir_children_num(sqlfs_t *sqlfs, const char *path)
{
    int i, r, count = 0;
    const char *tail;
    const char *t, *t2;
    char *lpath = 0;
    static const char *cmd = "select key from meta_data where key glob :pattern; ";
    char tmp[PATH_MAX];
    struct stat st;
    sqlite3_stmt *stmt;

    if ((i = key_is_dir(sqlfs, path)), (i == 0))
    {

        return 0;
    }
    else if (i == 2)
        return -EBUSY;

    lpath = strdup(path);
    remove_tail_slash(lpath);
    snprintf(tmp, sizeof(tmp), "%s/*", lpath);
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
    }
    else
    {
        sqlite3_bind_text(stmt, 1, tmp, -1, SQLITE_STATIC);

        while (1)
        {
            r = sql_step(stmt);
            if (r == SQLITE_ROW)
            {
                t = sqlite3_column_text(stmt, 0);
                t2 = t + strlen(tmp) - 1;
                if (strchr(t2, '/'))
                    continue; /* grand child, etc. */
                count++;

            }
            else if (r == SQLITE_DONE)
            {
                break;
            }
            else
            {
                show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

                break;
            }
        }
    }
    free(lpath);

    if (r == SQLITE_BUSY)
        count = -1;
    return count;
}

static int set_attr(sqlfs_t *sqlfs, const char *key, const key_attr *attr);


static int ensure_existence(sqlfs_t *sqlfs, const char *key, const char *type)
{
    int r;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    if (key_exists(sqlfs, key, 0) == 0)
    {
        attr.path = strdup(key);
        attr.type = strdup(type);
        attr.mode = get_sqlfs(sqlfs)->default_mode; /* to use default */
#ifdef FUSE
        attr.uid = geteuid();
        attr.gid = getegid();
#else
        attr.uid = get_sqlfs(sqlfs)->uid;
        attr.gid = get_sqlfs(sqlfs)->gid;

#endif
        attr.inode = get_new_inode();
        r = set_attr(sqlfs, key, &attr);
        if (r != SQLITE_OK)
        {
            clean_attr(&attr);
            return 0;

        }
        clean_attr(&attr);
        return 2;
    }

    return 1;

}
#if 0
static int ensure_parent_existence(sqlfs_t *sqlfs, const char *key)
{
    int r;


    char *parent = calloc(strlen(key) + 2, sizeof(char));

    char *t;
    assert(parent);

    strcpy(parent, key);
    remove_tail_slash(parent);
    t = strrchr(parent, '/');
    if (t)
    {

        *t = 0;
    }

    if (*parent == 0)
    {
        t = parent;
        *t = '/';
        *(t + 1) = 0;
    }
    ensure_existence(sqlfs, parent, TYPE_DIR);
    free(parent);
    return 1;
}
#endif

static int get_parent_path(const char *path, char buf[PATH_MAX])
{

    char *s;
    if ((path[0] == '/') && (path[1] == 0))
    {
        /* the root directory, which has no parent */
        return SQLITE_NOTFOUND;
    }

    strcpy(buf, path);
    remove_tail_slash(buf);
    s = strrchr(buf, '/');

    if (s == 0)
        return SQLITE_NOTFOUND; /* no parent? */
    if (s == buf)
        s[1] = 0;
    else
        s[0] = 0;
    return SQLITE_OK;

}


#undef INDEX
#define INDEX 16


static int get_permission_data(sqlfs_t *sqlfs, const char *key, gid_t *gid, uid_t *uid, mode_t *mode)
{

    int i, r;

    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd = "select mode, uid, gid from meta_data where key = :key; ";


    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return r;
    }
    r = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        else if (r == SQLITE_BUSY)
            ;
        else
            r = SQLITE_NOTFOUND;
    }
    else
    {
        *mode = (mode_t) (sqlite3_column_int(stmt, 0));
        *uid = (uid_t) (sqlite3_column_int(stmt, 1));
        *gid = (gid_t) (sqlite3_column_int(stmt, 2));
        r = SQLITE_OK;
    }

    sqlite3_reset(stmt);
    key_accessed(sqlfs, key);
    return r;

}

static int get_parent_permission_data(sqlfs_t *sqlfs, const char *key, gid_t *gid, uid_t *uid, mode_t *mode)
{
    char tmp[PATH_MAX], *s;
    int r;
    r = get_parent_path(key, tmp);
    if (r == SQLITE_OK)
        r = get_permission_data(sqlfs, tmp, gid, uid, mode);

    return r;
}


#undef INDEX
#define INDEX 17


static int get_attr(sqlfs_t *sqlfs, const char *key, key_attr *attr)
{
    int i, r;

    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd = "select key, type, mode, uid, gid, atime, mtime, ctime, size, inode from meta_data where key = :key; ";

    clean_attr(attr);
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return r;
    }
    r = sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        if (r == SQLITE_BUSY)
            ;
        else
            r = SQLITE_NOTFOUND;
    }
    else
    {
        attr->path = make_str_copy(sqlite3_column_text(stmt, 0));
        assert(!strcmp(key, attr->path));
        attr->type = make_str_copy(sqlite3_column_text(stmt, 1));
        attr->mode = (sqlite3_column_int(stmt, 2));
        attr->uid = (sqlite3_column_int(stmt, 3));
        attr->gid = (sqlite3_column_int(stmt, 4));
        attr->atime = (sqlite3_column_int(stmt, 5));
        attr->mtime = (sqlite3_column_int(stmt, 6));
        attr->ctime = (sqlite3_column_int(stmt, 7));
        attr->size = (sqlite3_column_int64(stmt, 8));
        attr->inode = (sqlite3_column_int(stmt, 9));
        r = SQLITE_OK;
    }

    sqlite3_reset(stmt);
    key_accessed(sqlfs, key);
    return r;

}


#undef INDEX
#define INDEX 18

static int set_attr(sqlfs_t *sqlfs, const char *key, const key_attr *attr)
{
    int i, r;
    const char *tail;
    sqlite3_stmt *stmt;
    int mode = attr->mode;
    static const char *cmd1 = "insert or ignore into meta_data (key) VALUES ( :key ) ; ";
    static const char *cmd2 = "update meta_data set type = :type, mode = :mode, uid = :uid, gid = :gid,"
                              "atime = :atime, mtime = :mtime, ctime = :ctime,  size = :size, inode = :inode, block_size = :block_size where key = :key; ";

    BEGIN
    if (!strcmp(attr->type, TYPE_DIR))
        mode |= S_IFDIR;
    else if (!strcmp(attr->type, TYPE_SYM_LINK))
        mode |= S_IFLNK;
    else
        mode |= S_IFREG;

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    sqlite3_reset(stmt);


#undef INDEX
#define INDEX 19


    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, attr->type, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, mode);
    sqlite3_bind_int(stmt, 3, attr->uid);
    sqlite3_bind_int(stmt, 4, attr->gid);
    sqlite3_bind_int(stmt, 5, attr->atime);
    sqlite3_bind_int(stmt, 6, attr->mtime);
    sqlite3_bind_int(stmt, 7, attr->ctime);
    sqlite3_bind_int64(stmt, 8, attr->size);
    sqlite3_bind_int(stmt, 9, attr->inode);
    sqlite3_bind_int(stmt, 10, BLOCK_SIZE);

    sqlite3_bind_text(stmt, 11, attr->path, -1, SQLITE_STATIC);
    r = sql_step(stmt);


    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    else
        r = SQLITE_OK;
    sqlite3_reset(stmt);
    key_modified(sqlfs, key);
    /*ensure_parent_existence(sqlfs, key);*/
    COMPLETE(1)
    return r;

}


#undef INDEX
#define INDEX 20


static int key_set_type(sqlfs_t *sqlfs, const char *key, const char *type)
{
    int r = SQLITE_OK, i;
    const char *tail;
    static const char *cmd = "update meta_data set type = :type where key = :key; ";
    sqlite3_stmt *stmt;

    BEGIN
    i = ensure_existence(sqlfs, key, type);
    if (i == 1)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            COMPLETE(1)
            return r;
        }
        sqlite3_bind_text(stmt, 1, type, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, key, -1, SQLITE_STATIC);
        r = sql_step(stmt);
        if (r != SQLITE_DONE)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        }
        sqlite3_reset(stmt);
    }
    else
        r = SQLITE_ERROR;
    COMPLETE(1)
    return r;
}



#undef INDEX
#define INDEX 21

static int get_value_block(sqlfs_t *sqlfs, const char *key, char *data, int block_no, int *size)
{
    int i, r;
    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd = "select data_block from value_data where key = :key and block_no = :block_no;";
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ;
    clean_attr(&attr);
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, block_no);
    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    else
    {
        if (size)
            *size = sqlite3_column_bytes(stmt, 0);
        memcpy(data, sqlite3_column_blob(stmt, 0), sqlite3_column_bytes(stmt, 0));
        r = SQLITE_OK;

    }

    sqlite3_reset(stmt);

    return r;

}


#undef INDEX
#define INDEX 22

static int set_value_block(sqlfs_t *sqlfs, const char *key, const char *data, int block_no, int size)
{
    int i, r;
    const char *tail;
    sqlite3_stmt *stmt;

    static const char *cmd = "update value_data set data_block = :data_block where key = :key and block_no = :block_no;";
    static const char *cmd1 = "insert or ignore into value_data (key, block_no) VALUES ( :key, :block_no ) ; ";
    static const char *cmd2 = "delete from value_data  where key = :key and block_no = :block_no;";
    char *tmp;
    BEGIN

    if (size == 0)
    {

        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt,  &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            COMPLETE(1)
            return r;
        }
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, block_no);
        r = sql_step(stmt);
        if (r != SQLITE_DONE)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        }
        else
            r = SQLITE_OK;

        sqlite3_reset(stmt);
        COMPLETE(1)
        return r;
    }


#undef INDEX
#define INDEX 23


    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, block_no);
    r = sql_step(stmt);
    sqlite3_reset(stmt);

    if (r == SQLITE_BUSY)
    {
        COMPLETE(1)
        return r;
    }

#undef INDEX
#define INDEX 24


    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_blob(stmt, 1, data, size, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, key, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 3, block_no);
    r = sql_step(stmt);


    if (r != SQLITE_DONE)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    else
        r = SQLITE_OK;
    sqlite3_reset(stmt);

    COMPLETE(1)
    return r;

}



#undef INDEX
#define INDEX 25


static int get_value(sqlfs_t *sqlfs, const char *key, key_value *value, size_t begin, size_t end)
{
    int i, r;
    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd = "select size from meta_data where key = :key; ";
    size_t size;
    int block_no;

    clean_value(value);
    BEGIN

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }
    else
    {
        size = sqlite3_column_int64(stmt, 0);
        r = SQLITE_OK;
    }
    if (r == SQLITE_OK)
    {
        if ((end == 0) || (end > size))
        {
            end = size;
        }
        if (begin < end)
        {
            value->size = size;
            value->data = malloc(value->size);
            assert(value->data);
            if (end == 0)
                end = value->size;
            begin = (block_no = (begin / BLOCK_SIZE)) * BLOCK_SIZE;
            for ( ; begin < end; begin += BLOCK_SIZE, block_no++)
            {

                r = get_value_block(sqlfs, key, value->data + begin - value->offset, block_no, NULL);
                if (r != SQLITE_OK)
                    break;
            }
        }
        else
            r = SQLITE_NOTFOUND  ;
    }

    sqlite3_reset(stmt);
    key_accessed(sqlfs, key);
    COMPLETE(1)
    return r;

}


#undef INDEX
#define INDEX 26

static int set_value(sqlfs_t *sqlfs, const char *key, const key_value *value, size_t begin, size_t end)
{
    int i, r;
    const char *tail;
    sqlite3_stmt *stmt;
    size_t begin2, end2, length;
    int block_no;
    static const char *cmd1 = "insert or ignore into meta_data (key) VALUES ( :key ) ; ";
    static const char *cmd2 = "update meta_data set size = :size where key =  :key  ; ";
    char *tmp;
    BEGIN
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    sqlite3_reset(stmt);

    if (r == SQLITE_BUSY)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }

#undef INDEX
#define INDEX 27



    {
        tmp = calloc( BLOCK_SIZE, sizeof(char));

        if (end == 0)
            end = value->size;
        begin2 = (block_no = (begin / BLOCK_SIZE)) * BLOCK_SIZE;
        end2 = end;
        if (end2 > begin2 + BLOCK_SIZE)
            end2 = begin2 + BLOCK_SIZE;

        {
            size_t old_size = 0;
            r = get_value_block(sqlfs, key, tmp, block_no, &old_size);
            length = end2 - begin;
            memcpy(tmp + (begin - begin2), (value->data - value->offset) + begin, length);
            length = end2 - begin2;
            if (length < old_size)
                length = old_size;
            r = set_value_block(sqlfs, key, tmp, block_no, length);
            block_no++;
            begin2 += BLOCK_SIZE;
        }
        for ( ; begin2 < end / BLOCK_SIZE * BLOCK_SIZE; begin2 += BLOCK_SIZE, block_no++)
        {

            r = set_value_block(sqlfs, key, (value->data - value->offset) + BLOCK_SIZE * block_no, block_no, BLOCK_SIZE);
            if (r != SQLITE_OK)
                break;
        }

        if (begin2 < end)
        {

            assert(begin2 % BLOCK_SIZE == 0);
            assert(end - begin2 < (size_t) BLOCK_SIZE);

            memset(tmp, 0, BLOCK_SIZE);
            r = get_value_block(sqlfs, key, tmp, block_no, &i);
            if (r != SQLITE_OK)
                i = 0;
            memcpy(tmp, (value->data - value->offset) + begin2, end - begin2 );
            if (i < (int)(end - begin2))
                i = (int)(end - begin2);

            r = set_value_block(sqlfs, key, tmp, block_no, i);
        }

        free(tmp);
    }
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        COMPLETE(1)
        return r;
    }
    sqlite3_bind_int64(stmt, 1, value->size);
    sqlite3_bind_text(stmt, 2, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    sqlite3_reset(stmt);
    if (r == SQLITE_DONE)
        r = SQLITE_OK;
    key_modified(sqlfs, key);
    /*ensure_parent_existence(sqlfs, key);*/
    COMPLETE(1)
    return r;

}


#undef INDEX
#define INDEX 28


static int key_shorten_value(sqlfs_t *sqlfs, const char *key, size_t new_length)
{
    int r, i;
    size_t l;
    int block_no;
    char *tmp;
    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd1 = "delete from value_data where key = :key and block_no > :block_no; ";
    static const char *cmd2 = "update meta_data set size = :size where key =  :key  ; ";

    BEGIN
    if ((i = key_exists(sqlfs, key, &l)), (i == 0))
    {
        assert(0);
        show_msg(stderr, "Illegal truncateion on non-existence key %s\n", key);
        COMPLETE(1)
        return SQLITE_ERROR;
    }
    else if (i == 2)
    {
        COMPLETE(1)
        return SQLITE_BUSY;
    }

    assert(l > new_length);
    block_no = new_length / BLOCK_SIZE;

    tmp = calloc(BLOCK_SIZE, sizeof(char));
    assert(tmp);
    r = get_value_block(sqlfs, key, tmp, block_no, &i);
    assert(new_length % BLOCK_SIZE <= (unsigned int) i);
    r = set_value_block(sqlfs, key, tmp, block_no, new_length % BLOCK_SIZE);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

    }

    if (r == SQLITE_OK)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt,  &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

        }
        else
        {
            sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
            sqlite3_bind_int(stmt, 2, block_no);
            r = sql_step(stmt);
            /*if (r != SQLITE_DONE)
            {
                show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

            }*/
            /*ignore the result */
            if (r == SQLITE_BUSY)
                ;
            else
                r = SQLITE_OK;
            sqlite3_reset(stmt);
        }
    }


#undef INDEX
#define INDEX 29

    if (r == SQLITE_OK)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt,  &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

        }
        else
        {
            sqlite3_bind_int64(stmt, 1, new_length);
            sqlite3_bind_text(stmt, 2, key, -1, SQLITE_STATIC);
            r = sql_step(stmt);
            sqlite3_reset(stmt);
            if (r == SQLITE_DONE)
                r = SQLITE_OK;
        }

    }
    free(tmp);
    key_modified(sqlfs, key);
    /*ensure_parent_existence(sqlfs, key);*/
    COMPLETE(1)
    return r;

}

static int check_parent_access(sqlfs_t *sqlfs, const char *path)
{
    char ppath[PATH_MAX];

    int r, result = 0;

    BEGIN
    r = get_parent_path(path, ppath);
//fprintf(stderr, "%s #1 returns %d on %s\n", __func__, r, path);//???
    if (r == SQLITE_OK)
    {
        result = check_parent_access(sqlfs, ppath);
//fprintf(stderr, "%s #2 returns %d on %s\n", __func__, result, path);//???
        if (result == 0)
            result = (sqlfs_proc_access(sqlfs, (ppath), X_OK));
//fprintf(stderr, "%s #3 returns %d on %s %s\n", __func__, result, path, ppath);//???
    }
    /* else if no parent, we return 0 by default */

    COMPLETE(1)
//fprintf(stderr, "%s returns %d on %s\n", __func__, result, path);//???
    return result;
}


static int check_parent_write(sqlfs_t *sqlfs, const char *path)
{
    char ppath[PATH_MAX];

    int r, result = 0;

    BEGIN
    r = get_parent_path(path, ppath);
    if (r == SQLITE_OK)
    {
        result = (sqlfs_proc_access(sqlfs, (ppath), W_OK | X_OK));
//fprintf(stderr, "check directory write 1st %s %d uid %d gid %d\n",   ppath, result, get_sqlfs(sqlfs)->uid, get_sqlfs(sqlfs)->gid);//???

#ifndef FUSE
        if (result == -ENOENT)
        {
            result = check_parent_write(sqlfs, ppath);
            if (result == 0)
                ensure_existence(sqlfs, ppath, TYPE_DIR);
            result = (sqlfs_proc_access(sqlfs, (ppath), W_OK | X_OK));
        }
#endif
    }
    COMPLETE(1)
//fprintf(stderr, "check directory write %s %d\n",   ppath, result);//???
    return result;
}

#define CHECK_PARENT_PATH(p) result = check_parent_access(sqlfs, (p)); if (result != 0) { COMPLETE(1); return result; }

#define CHECK_READ(p)  result = (sqlfs_proc_access(sqlfs, (p), R_OK | F_OK));  if (result != 0) { COMPLETE(1); return result; }
#define CHECK_WRITE(p) result = (sqlfs_proc_access(sqlfs, (p), W_OK | F_OK));  if (result != 0) { COMPLETE(1); return result; }

#define CHECK_DIR_WRITE(p) result = (sqlfs_proc_access(sqlfs, (p), W_OK | F_OK | X_OK));  if (result != 0) { COMPLETE(1); return result; }
#define CHECK_DIR_READ(p) result = (sqlfs_proc_access(sqlfs, (p), R_OK | F_OK | X_OK));  if (result != 0) {fprintf(stderr, "dir read failed %d\n", result); COMPLETE(1); return result; }

#define CHECK_PARENT_READ(p)  \
  { char ppath[PATH_MAX]; if (SQLITE_OK == get_parent_path((p), ppath))  {  result = (sqlfs_proc_access(sqlfs, (ppath), R_OK | X_OK));  if (result != 0) { COMPLETE(1); return result; }}}
#define CHECK_PARENT_WRITE(p) \
  { result = check_parent_write(sqlfs, (p));  if (result != 0) { COMPLETE(1); return result; }}

int sqlfs_proc_getattr(sqlfs_t *sqlfs, const char *path, struct stat *stbuf)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    int r, result = 0;

    BEGIN
    CHECK_PARENT_PATH(path)
    CHECK_READ(path)

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK)
    {
        memset(stbuf, 0, sizeof(*stbuf));
        stbuf->st_mode = attr.mode;
        if (!strcmp(attr.type, TYPE_DIR))
            stbuf->st_mode |= S_IFDIR;
        else if (!strcmp(attr.type, TYPE_SYM_LINK))
            stbuf->st_mode |= S_IFLNK;
        else
            stbuf->st_mode |= S_IFREG;
        stbuf->st_nlink = 1;
        stbuf->st_uid = (uid_t) attr.uid;
        stbuf->st_gid = (gid_t) attr.gid;
        stbuf->st_size = (off_t) attr.size;
        stbuf->st_blksize = 512;
        stbuf->st_blocks = attr.size / 512;
        stbuf->st_atime = attr.atime;
        stbuf->st_mtime = attr.mtime;
        stbuf->st_ctime = attr.ctime;
        stbuf->st_ino = attr.inode;
        clean_attr(&attr);

    }
    else
        result =   -ENOENT;
    COMPLETE(1)
    return result;
}

static int gid_in_supp_groups(gid_t gid)
{
    int n, num_groups = getgroups(0, 0);
    int r = 0;
    if (num_groups)
    {
        gid_t *gids = malloc(sizeof(gids[0]) * num_groups);
        n = getgroups(num_groups, gids);

        assert(n == num_groups);
        for (n = 0; n < num_groups; n++)
        {
            if (gid == gids[n])
            {
                r = 1;
                break;
            }
        }
        free(gids);
    }
    return r;
}


int sqlfs_proc_access(sqlfs_t *sqlfs, const char *path, int mask)
{

    int i, r = SQLITE_OK, result = 0;
#ifdef FUSE
    gid_t gid = getegid();
    uid_t uid = geteuid();
#else
    gid_t gid = get_sqlfs(sqlfs)->gid;
    uid_t uid = get_sqlfs(sqlfs)->uid;
#endif
    uid_t fuid;
    gid_t fgid;
    mode_t fmode;

    BEGIN

    if (uid == 0) /* root user so everything is granted */
    {
        if ((i = key_exists(sqlfs, path, 0)), !i)
            result = -ENOENT;
        else if (i == 2)
            result = -EBUSY;

        COMPLETE(1)
        //fprintf(stderr, "root access returns %d on %s\n", result, path);//???
        return result;
    }

    if (mask & F_OK)
    {
        r = get_parent_permission_data(sqlfs, path, &fgid, &fuid, &fmode);
        if (r == SQLITE_OK)
        {
            if (uid == (uid_t) fuid)
            {
                if ( !(S_IRUSR  & S_IXUSR & fmode))
                {
                    result = -EACCES;
                }

            }
            else if ((gid == (gid_t) fgid) || (gid_in_supp_groups(fgid)))
            {
                if ( !(S_IRGRP  & S_IXGRP & fmode))
                {
                    result = -EACCES;
                }
            }
            else
            {
                if ( !(S_IROTH  & S_IXOTH & fmode))
                {
                    result = -EACCES;
                }
            }
        }
        else if (r == SQLITE_NOTFOUND)
            result = -ENOENT;

    }


    if (result == 0)
        r = get_permission_data(get_sqlfs(sqlfs), path, &fgid, &fuid, &fmode);
    //fprintf(stderr, "get permission returns %d\n", r);//???
    if ((r == SQLITE_OK) && (result == 0))
    {
        if (uid == (uid_t) fuid)
        {
            if (((mask & R_OK) && !(S_IRUSR & fmode))  ||
                    ((mask & W_OK) && !(S_IWUSR & fmode))  ||
                    ((mask & X_OK) && !(S_IXUSR & fmode)))
            {

                result = -EACCES;
            }
        }
        else if ((gid == (uid_t) fgid) || (gid_in_supp_groups(fgid)))
        {
            if (((mask & R_OK) && !(S_IRGRP & fmode))  ||
                    ((mask & W_OK) && !(S_IWGRP & fmode))  ||
                    ((mask & X_OK) && !(S_IXGRP & fmode)))
            {

                result = -EACCES;
            }
        }

        else if (((mask & R_OK) && !(S_IROTH & fmode))  ||
                 ((mask & W_OK) && !(S_IWOTH & fmode))  ||
                 ((mask & X_OK) && !(S_IXOTH & fmode)))
        {

            result = -EACCES;
        }


    }
    else if (r == SQLITE_NOTFOUND)
        result = -ENOENT;
    else
        result = -EIO;

    COMPLETE(1)
    return result;
}


int sqlfs_proc_readlink(sqlfs_t *sqlfs, const char *path, char *buf, size_t size)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    key_value value = { 0, 0, 0 };
    int r, result = 0;
    BEGIN
    CHECK_PARENT_PATH(path)
    CHECK_READ(path)
    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK)
    {
        if (!strcmp(attr.type, TYPE_SYM_LINK))
        {
            r = get_value(get_sqlfs(sqlfs), path, &value, 0, 0);
            if (r == SQLITE_OK)
            {
                if (value.size > size)
                { /* too short a buffer */
                    show_msg(stderr,
                             "warning: readlink provided buffer too small\n");

                }
                strncpy(buf, value.data, size);
            }
            clean_value(&value);
        }
        else
        {
            result = -EINVAL;
        }
    }
    clean_attr(&attr);
    COMPLETE(1)
    return result;
}


#undef INDEX
#define INDEX 30

int sqlfs_proc_readdir(sqlfs_t *sqlfs, const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
    int i, r, result = 0;
    const char *tail;
    const char *t, *t2;
    static const char *cmd = "select key, mode from meta_data where key glob :pattern; ";
    char tmp[PATH_MAX];
    char *lpath;
    sqlite3_stmt *stmt;
    BEGIN
    CHECK_PARENT_PATH(path)
    CHECK_DIR_READ(path)

    if ((i = key_is_dir(get_sqlfs(sqlfs), path)), !i)
    {
        COMPLETE(1)
        return -ENOTDIR;
    }
    else if (i == 2)
    {
        COMPLETE(1)
        return -EBUSY;
    }

    lpath = strdup(path);
    remove_tail_slash(lpath);
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    snprintf(tmp, sizeof(tmp), "%s/*", lpath);

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

        result = -EACCES;
    }
    if (result == 0)
    {
        sqlite3_bind_text(stmt, 1, tmp, -1, SQLITE_STATIC);

        while (1)
        {
            r = sql_step(stmt);
            if (r == SQLITE_ROW)
            {
                t = sqlite3_column_text(stmt, 0);
                if (!strcmp(t, lpath))
                    continue;
                t2 = t + strlen(lpath) + 1;
                if (strchr(t2, '/'))
                    continue; /* grand child, etc. */
                if (*t2 == 0) /* special case when dir the root directory */
                    continue;

                if (filler(buf, t2, NULL, 0))
                    break;
            }
            else if (r == SQLITE_DONE)
            {
                break;
            }
            else if (r == SQLITE_BUSY)
                result = -EBUSY;
            else
            {
                show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
                result = -EACCES;
                break;
            }
        }
        sqlite3_reset(stmt);
    }
    COMPLETE(1)
    free(lpath);
    return result;
}

int sqlfs_proc_mknod(sqlfs_t *sqlfs, const char *path, mode_t mode, dev_t rdev)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    int r, result = 0;
    if ((S_IFCHR & mode) || (S_IFBLK & mode))
        return -EACCES; /* not supported, not allowed */

    if (!((S_IFREG & mode) || (S_IFIFO & mode) || (S_IFSOCK & mode)))
        return -EINVAL;
    BEGIN
    CHECK_PARENT_WRITE(path)

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK)
    {
        clean_attr(&attr);
        COMPLETE(1)
        return -EEXIST;
    }
    attr.path = strdup(path);
    attr.type = strdup(TYPE_BLOB);
    attr.mode = mode;
#ifdef FUSE
    attr.gid = getegid();
    attr.uid = geteuid();
#else
    attr.gid = get_sqlfs(sqlfs)->gid;
    attr.uid = get_sqlfs(sqlfs)->uid;
#endif
    attr.size = 0;
    attr.inode = get_new_inode();
    r = set_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_BUSY)
        result = -EBUSY;
    else if (r != SQLITE_OK)
    {

        result =  -EINVAL;
    }
    clean_attr(&attr);
    COMPLETE(1)
    return result;
}

int sqlfs_proc_mkdir(sqlfs_t *sqlfs, const char *path, mode_t mode)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    int r, result = 0;
    BEGIN
    CHECK_PARENT_WRITE(path)

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK)
    {
        clean_attr(&attr);
        COMPLETE(1)
        return -EEXIST;
    }
    attr.path = strdup(path);
    attr.type = strdup(TYPE_DIR);
    attr.mode = mode;

#ifdef FUSE
    attr.gid = getegid();
    attr.uid = geteuid();
#else
    attr.gid = get_sqlfs(sqlfs)->gid;
    attr.uid = get_sqlfs(sqlfs)->uid;
#endif
    attr.size = 0;
    attr.inode = get_new_inode();
    r = set_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_BUSY)
        result = -EBUSY;
    else  if (r != SQLITE_OK)
    {
        result = -EINVAL;
    }
    clean_attr(&attr);
    COMPLETE(1)
    return result;
}

int sqlfs_proc_unlink(sqlfs_t *sqlfs, const char *path)
{
    int i, r, result = 0;
    BEGIN
    CHECK_PARENT_WRITE(path)
    if ((i = key_exists(get_sqlfs(sqlfs), path, 0)), (i == 0))
        result = -ENOENT;
    else if (i == 2)
        result = -EBUSY;

    if (key_is_dir(get_sqlfs(sqlfs), path))
    {
        result = -EISDIR;
    }


    if (result == 0)
    {
        r = remove_key(get_sqlfs(sqlfs), path);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
            result = -EIO;
    }
    COMPLETE(1)
    return result;
}

int sqlfs_proc_rmdir(sqlfs_t *sqlfs, const char *path)
{
    int r, result = 0;
    BEGIN
    CHECK_PARENT_WRITE(path)

    if (get_dir_children_num(get_sqlfs(sqlfs), path) > 0)
    {
        result = -ENOTEMPTY;
    }
    else
    {
        r = remove_key(get_sqlfs(sqlfs), path);
        if (r != SQLITE_OK)
            result = -EIO;
    }
    COMPLETE(1)
    return result;
}

int sqlfs_proc_symlink(sqlfs_t *sqlfs, const char *path, const char *to)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    key_value value = { 0, 0, 0 };
    int r, result = 0;
    BEGIN
    CHECK_PARENT_WRITE(to)

    r = get_attr(get_sqlfs(sqlfs), to, &attr);
    if (r == SQLITE_OK)
    {
        clean_attr(&attr);
        COMPLETE(1)
        return -EEXIST;
    }

    attr.path = strdup(to);
    attr.type = strdup(TYPE_SYM_LINK);
    attr.mode = get_sqlfs(sqlfs)->default_mode; /* 0777 ?? */
#ifdef FUSE
    attr.uid = geteuid();
    attr.gid = getegid();
#else
    attr.uid = get_sqlfs(sqlfs)->uid;
    attr.gid = get_sqlfs(sqlfs)->gid;

#endif
    attr.size = 0;
    attr.inode = get_new_inode();
    r = set_attr(get_sqlfs(sqlfs), to, &attr);

    if (r != SQLITE_OK)
    {
        clean_attr(&attr);
        COMPLETE(1)
        if (r == SQLITE_BUSY)
            return -EBUSY;
        return -EINVAL;
    }
    clean_attr(&attr);
    value.data = strdup(path);
    value.size = strlen(value.data) + 1;

    r = set_value(get_sqlfs(sqlfs), to, &value, 0, 0);
    if (r != SQLITE_OK)
    {
        result = -EIO;
    }

    clean_value(&value);
    COMPLETE(1)
    return result;

}

int sqlfs_proc_rename(sqlfs_t *sqlfs, const char *from, const char *to)
{

    int i, r = SQLITE_OK, result = 0;
    BEGIN
    CHECK_PARENT_WRITE(from)
    CHECK_PARENT_WRITE(to)

    if ((i = key_exists(get_sqlfs(sqlfs), from, 0)), !i)
    {
        COMPLETE(1)

        return -EIO;
    }
    else if (i == 2)
    {

        COMPLETE(1)
        return -EBUSY;
    }

    if (key_is_dir(get_sqlfs(sqlfs), to) == 1)
    {
        if (get_dir_children_num(get_sqlfs(sqlfs), to) > 0)
        {

            result =  -ENOTEMPTY;
        }
        if (!key_is_dir(get_sqlfs(sqlfs), from))
        {

            result = -EISDIR;
        }
    }
    if ((result == 0) && (key_is_dir(get_sqlfs(sqlfs), from) == 1))
    {
        if (key_is_dir(get_sqlfs(sqlfs), to) == 0)
        {

            result = -ENOTDIR;
        }
    }

    if ((i = key_exists(get_sqlfs(sqlfs), to, 0)), (i == 1))
    {
        r = remove_key(get_sqlfs(sqlfs), to);

        if (r != SQLITE_OK)
        {
            result = -EIO;
            if (r == SQLITE_BUSY)
                result = -EBUSY;


        }
    }
    else if (i == 2)
        result = -EBUSY;

    if (result == 0)
    {
        r = rename_key(get_sqlfs(sqlfs), from, to);

        if (r != SQLITE_OK)
        {
            result = -EIO;
            if (r == SQLITE_BUSY)
                result = -EBUSY;


        }

    }
    COMPLETE(1)
    return result;
}

int sqlfs_proc_link(sqlfs_t *sqlfs, const char *from, const char *to)
{ /* hard link not supported, not allowed */
    return - EACCES;

}

int sqlfs_proc_chmod(sqlfs_t *sqlfs, const char *path, mode_t mode)
{
    int r, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ;
    BEGIN

    CHECK_PARENT_PATH(path)

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r != SQLITE_OK)
    {
        COMPLETE(1)
        clean_attr(&attr);
        if (r == SQLITE_BUSY)
            return -EBUSY;
        return -ENOENT;
    }
#ifdef FUSE
    if ((geteuid() != 0) && (geteuid() != (uid_t) attr.uid))
#else
    if ((get_sqlfs(sqlfs)->uid != 0) && (get_sqlfs(sqlfs)->uid != (uid_t) attr.uid))
#endif
    {
        result = -EACCES;
    }
    else
    {
        attr.mode &= ~(S_IRWXU | S_IRWXG | S_IRWXO);
        attr.mode |= mode;

        r = set_attr(get_sqlfs(sqlfs), path, &attr);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
        {
            result = -EACCES;
        }
    }

    clean_attr(&attr);
    COMPLETE(1)
    return result;
}

int sqlfs_proc_chown(sqlfs_t *sqlfs, const char *path, uid_t uid, gid_t gid)
{
    int r, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ;
    int group_change_only = 0;
    BEGIN

    CHECK_PARENT_PATH(path)

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r != SQLITE_OK)
    {
        COMPLETE(1)
        clean_attr(&attr);
        if (r == SQLITE_BUSY)
            return -EBUSY;
        return -ENOENT;
    }
#ifdef FUSE
    if ((geteuid() == 0) || ((geteuid() == attr.uid) && (uid == (uid_t) attr.uid)))
#else
    if ((get_sqlfs(sqlfs)->uid == 0) || ((get_sqlfs(sqlfs)->uid == (uid_t) attr.uid) && (uid == (uid_t) attr.uid)))
#endif
    {
        attr.uid = uid;
        attr.gid = gid;

        r = set_attr(get_sqlfs(sqlfs), path, &attr);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
        {
            result = -EACCES;
        }
    }
    else
    {
        result = -EACCES;
    }
    clean_attr(&attr);
    COMPLETE(1)
    return result;

}

int sqlfs_proc_truncate(sqlfs_t *sqlfs, const char *path, off_t size)
{
    int r, result = 0;
    char *data;
    key_value value = { 0, 0, 0 };


    BEGIN
    CHECK_PARENT_PATH(path)
    CHECK_WRITE(path)
    r = get_value(get_sqlfs(sqlfs), path, &value, 0, 0);

    if (r == SQLITE_NOTFOUND)
    {
        r = 0;
    }
    else
        if (r != SQLITE_OK)
        {
            COMPLETE(1)
            clean_value(&value);
            if (r == SQLITE_BUSY)
                return -EBUSY;
            return -ENOENT;
        }

    if (value.size > (size_t) size)
    {
        value.size = size;
        r = key_shorten_value(get_sqlfs(sqlfs), path, value.size);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
            result = -EIO;
    }
    else if (value.size < (size_t) size)
    {
        data = realloc(value.data, size);
        if (data == NULL)
        {
            result = -EINVAL;
        }
        else
        {
            memset(data + value.size, 0, size - value.size);
            value.data = data;
            value.size = size;
        }
        if (result == 0)
        {
            r = set_value(get_sqlfs(sqlfs), path, &value, 0, 0);
            if (r != SQLITE_OK)
            {
                if (r == SQLITE_BUSY)
                    result = -EBUSY;
                else result = -EACCES;
            }
        }
    }
    clean_value(&value);
    COMPLETE(1)
    return result;

}

int sqlfs_proc_utime(sqlfs_t *sqlfs, const char *path, struct utimbuf *buf)
{
    int r, result = 0;
    time_t now;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ;
    BEGIN
    CHECK_PARENT_PATH(path)
    CHECK_WRITE(path)
    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r != SQLITE_OK)
    {
        COMPLETE(1)
        clean_attr(&attr);
        if (r == SQLITE_BUSY)
            return -EBUSY;
        return -ENOENT;
    }
    if (!buf)
    {
        time(&now);
        attr.atime = now;
        attr.mtime = now;
    }
    else
    {

        attr.atime = buf->actime;
        attr.mtime = buf->modtime;
    }
    r = set_attr(get_sqlfs(sqlfs), path, &attr);
    if (r != SQLITE_OK)
    {
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else result = -EACCES;
    }

    clean_attr(&attr);
    COMPLETE(1)
    return result;

}

int sqlfs_proc_create(sqlfs_t *sqlfs, const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int r, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    if (fi->direct_io)
        return  -EACCES;

    fi->flags |= O_CREAT | O_WRONLY | O_TRUNC;
    BEGIN
    CHECK_PARENT_WRITE(path)

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK) /* already exists */
    {
        if (fi->flags & (O_CREAT | O_EXCL))
        {

            result = -EEXIST;
        }
        else
            if (!strcmp(attr.type, TYPE_DIR) && (fi->flags & (O_WRONLY | O_RDWR)))
            {

                result = -EISDIR;
            }
    }
    else
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else
            /* does not exist */
            if ((fi->flags & O_CREAT) == 0)
                result = - ENOENT ;
    if (result == 0)
    {
        attr.mode = mode;
        if (attr.path == 0)
        {
            attr.path = strdup(path);
            attr.inode = get_new_inode();
        }
        if (attr.type == 0)
            attr.type = strdup(TYPE_BLOB);
        r = set_attr(get_sqlfs(sqlfs), path, &attr);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
            result = -EACCES;
    }
    clean_attr(&attr);
    COMPLETE(1)
    return result;

}

int sqlfs_proc_open(sqlfs_t *sqlfs, const char *path, struct fuse_file_info *fi)
{
    int r, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    if (fi->direct_io)
        return  -EACCES;
    BEGIN

    if ((fi->flags & O_CREAT) )
    {
        CHECK_PARENT_WRITE(path)
    }

    if (fi->flags & (O_WRONLY | O_RDWR))
    {
        CHECK_PARENT_PATH(path)
        CHECK_WRITE(path)
    }
    else
    {
        CHECK_PARENT_PATH(path)
        CHECK_READ(path)
    }
    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK) /* already exists */
    {
        if (fi->flags & (O_CREAT | O_EXCL))
        {

            result = -EEXIST;
        }
        else
            if (!strcmp(attr.type, TYPE_DIR) && (fi->flags & (O_WRONLY | O_RDWR)))
            {

                result = -EISDIR;
            }
    }
    else
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else
            /* does not exist */
            if ((fi->flags & O_CREAT) == 0)
                result = - ENOENT ;
    if ((result == 0) && (fi->flags & O_CREAT))
    {
        attr.mode = get_sqlfs(sqlfs)->default_mode; /* to use some kind of default */
#ifdef FUSE
        attr.uid = geteuid();
        attr.gid = getegid();
#else
        attr.uid = get_sqlfs(sqlfs)->uid;
        attr.gid = get_sqlfs(sqlfs)->gid;

#endif
        if (attr.path == 0)
        {
            attr.path = strdup(path);
            attr.inode = get_new_inode();
        }
        if (attr.type == 0)
            attr.type = strdup(TYPE_BLOB);
        r = set_attr(get_sqlfs(sqlfs), path, &attr);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else
            if (r != SQLITE_OK)
                result = -EACCES;
    }
    clean_attr(&attr);
    COMPLETE(1)
    return result;
}

int sqlfs_proc_read(sqlfs_t *sqlfs, const char *path, char *buf, size_t size, off_t offset, struct
                    fuse_file_info *fi)
{
    int i, r, result = 0;
    int64_t length = size;
    key_value value = { 0, 0, 0 };

    BEGIN
    CHECK_PARENT_PATH(path)
    CHECK_READ(path)

    if (i = key_is_dir(get_sqlfs(sqlfs), path), (i == 1))
    {
        COMPLETE(1)
        return -EISDIR;
    }
    else if (i == 2)
    {
        COMPLETE(1)
        return -EBUSY;
    }

    /*if (fi)
    if ((fi->flags & (O_RDONLY | O_RDWR)) == 0)
        return - EBADF;*/

    r = get_value(get_sqlfs(sqlfs), path, &value, offset, offset + size);
    if (r != SQLITE_OK)
    {
        result = -EIO;
    }
    else if ((size_t) offset > value.size) /* nothing to read */
        result = 0;
    else
    {
        if (length > (int64_t) value.size - offset)
            length = (int64_t) value.size - offset;
        if (length < 0)
            length = 0;
        if (length > 0)
        {
            memcpy(buf, ((char*)value.data) + offset,  length);
        }
        result = length;
    }
    clean_value(&value);
    COMPLETE(1)
    return result;
}

int sqlfs_proc_write(sqlfs_t *sqlfs, const char *path, const char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
    int i, r, result = 0;
    char *data;
    size_t length = size, orig_size = 0;
    key_value value = { 0, 0, 0 };

    BEGIN

    if (i = key_is_dir(get_sqlfs(sqlfs), path), (i == 1))
    {
        COMPLETE(1)
        return -EISDIR;
    }
    else if (i == 2)
    {
        COMPLETE(1)
        return -EBUSY;
    }

    /*if (fi)
    if ((fi->flags & (O_WRONLY | O_RDWR)) == 0)
        return - EBADF;*/

    if ((i = key_exists(get_sqlfs(sqlfs), path, &orig_size) == 0), (i == 1))
    {
        key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        CHECK_PARENT_WRITE(path)
        attr.path = strdup(path);
        attr.type = strdup(TYPE_BLOB);
        attr.mode = get_sqlfs(sqlfs)->default_mode; /* use default mode */
#ifdef FUSE
        attr.uid = geteuid();
        attr.gid = getegid();
#else
        attr.uid = get_sqlfs(sqlfs)->uid;
        attr.gid = get_sqlfs(sqlfs)->gid;

#endif
        attr.inode = get_new_inode();
        r = set_attr(get_sqlfs(sqlfs), path, &attr);
        if (r != SQLITE_OK)
            result = -EIO;
        clean_attr(&attr);
        clean_value(&value);
    }
    else if (i == 2)
    {
        result = -EBUSY;
    }
    else
    {
        CHECK_PARENT_PATH(path)
        CHECK_WRITE(path)
    }
    if (result == 0)
    {
        if (orig_size)
        {
            value.data = calloc(orig_size, sizeof(char));
            value.size = orig_size;
        }
        if (length > value.size - offset)
        {
            data = realloc(value.data, offset + length);
            if (!data)
                result = -EFBIG;
            else
            {
                memset(data + value.size, 0, offset + length - value.size);
                value.data = data;
                value.size = offset + length;
            }
        }
        if (result == 0)
        {
            memcpy(value.data + offset, buf, length);

            result = length;
        }

        if ((size_t) offset > orig_size)
        {
            length += offset - orig_size;
            offset = orig_size; /* fill in the hole */
        }
        r = set_value(get_sqlfs(sqlfs), path, &value, offset, offset + length);
        if (r != SQLITE_OK)
            result = -EIO;
    }
    clean_value(&value);
    COMPLETE(1)
    return result;
}

int sqlfs_proc_statfs(sqlfs_t *sqlfs, const char *path, struct statvfs *stbuf)
{
    /* file system information not supported as we have no idea how
    to map that to a SQLite file */
    return -ENOSYS;
}

int sqlfs_proc_release(sqlfs_t *sqlfs, const char *path, struct fuse_file_info *fi)
{
    /* no operation */
    return 0;
}

int sqlfs_proc_fsync(sqlfs_t *sqlfs, const char *path, int isfdatasync, struct fuse_file_info *fi)
{
    sync(); /* just to sync everything */
    return 0;
}

/* xattr operations are optional and can safely be left unimplemented */
int sqlfs_proc_setxattr(sqlfs_t *sqlfs, const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
    return -ENOSYS;

}

int sqlfs_proc_getxattr(sqlfs_t *sqlfs, const char path, const char *name, char *value, size_t size)
{
    return -ENOSYS;
}

int sqlfs_proc_listxattr(sqlfs_t *sqlfs, const char *path, char *list, size_t size)
{
    return -ENOSYS;
}

int sqlfs_proc_removexattr(sqlfs_t *sqlfs, const char *path, const char *name)
{
    return -ENOSYS;
}


int sqlfs_del_tree(sqlfs_t *sqlfs, const char *key)
{
    int i, result = 0;
    BEGIN
    CHECK_PARENT_WRITE(key)
    CHECK_DIR_WRITE(key)

    if ((i = key_exists(get_sqlfs(sqlfs), key, 0)), (i == 0))
        result = -ENOENT;
    else if (i == 2)
        result = -EBUSY;

    if (result == 0)
    {
        if (SQLITE_OK == remove_key_subtree(get_sqlfs(sqlfs), key))
            result = 0;
        else
            result = -EIO;
    }
    COMPLETE(1)
    return result;
}


int sqlfs_del_tree_with_exclusion(sqlfs_t *sqlfs, const char *key, const char *exclusion_pattern)
{
    int i, result = 0;
    BEGIN
    CHECK_PARENT_WRITE(key)
    CHECK_DIR_WRITE(key)

    if ((i = key_exists(get_sqlfs(sqlfs), key, 0)) == 0)
        result = -ENOENT;
    else if (i == 2)
        result = -EBUSY;

    if (result == 0)
    {
        if (SQLITE_OK == remove_key_subtree_with_exclusion(get_sqlfs(sqlfs), key, exclusion_pattern))
            result = 0;
        else
            result = -EIO;
    }
    COMPLETE(1)
    return result;
}



int sqlfs_get_value(sqlfs_t *sqlfs, const char *key, key_value *value,
                    size_t begin, size_t end)
{
    int r = SQLITE_OK;
    BEGIN
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else
        if (sqlfs_proc_access(sqlfs, key, R_OK | F_OK) != 0)
            r = SQLITE_ERROR;
        else
            r = get_value(get_sqlfs(sqlfs), key, value, begin, end);

    COMPLETE(1)
    if (r == SQLITE_NOTFOUND)
        return -1;
    return SQLITE_OK == r;
}

int sqlfs_set_value(sqlfs_t *sqlfs, const char *key, const key_value *value,
                    size_t begin,  size_t end)
{
    int r = SQLITE_OK;
    BEGIN
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else
        if (sqlfs_proc_access(sqlfs, key, W_OK | F_OK) != 0)
            r = SQLITE_ERROR;
        else
            r = set_value(get_sqlfs(sqlfs), key, value, begin, end);
    COMPLETE(1)
    return SQLITE_OK == r;
}

int sqlfs_get_attr(sqlfs_t *sqlfs, const char *key, key_attr *attr)
{
    int i, r = 1;
    BEGIN
    if ((i = check_parent_access(sqlfs, key)) != 0)
    {
        if (i == -ENOENT)
            r = -1;
        else if (i == -EACCES)
            r = -2;
        else r = -1;
    }
    else
        if ((i = sqlfs_proc_access(sqlfs, key, R_OK | F_OK)) != 0)
        {
            if (i == -ENOENT)
                r = -1;
            else if (i == -EACCES)
                r = -2;
            else r = -1;
        }
        else
        {
            i = get_attr(get_sqlfs(sqlfs), key, attr);
            if (i == SQLITE_OK)
                r = 1;
            else if (i == SQLITE_NOTFOUND)
                r = -1;
        }
    COMPLETE(1)
//fprintf(stderr, "return %d on %s\n", r, key);//???
    return r;

}

int sqlfs_set_attr(sqlfs_t *sqlfs, const char *key, const key_attr *attr)
{
    int r = SQLITE_OK;

    BEGIN
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else
        if (sqlfs_proc_access(sqlfs, key, W_OK | F_OK) != 0)
            r = SQLITE_ERROR;
        else
            r = set_attr(get_sqlfs(sqlfs), key, attr);

    COMPLETE(1)
    return SQLITE_OK == r;
}

int sqlfs_begin_transaction(sqlfs_t *sqlfs)
{
    int r = SQLITE_OK;
    r = begin_transaction(get_sqlfs(sqlfs));
    if (r == SQLITE_BUSY)
        return 2;
    return SQLITE_OK == r;
}


int sqlfs_complete_transaction(sqlfs_t *sqlfs, int i)
{
    int r = SQLITE_OK;
    r = commit_transaction(get_sqlfs(sqlfs), i);
    if (r == SQLITE_BUSY)
        return 2;

    return SQLITE_OK == r;
}


int sqlfs_break_transaction(sqlfs_t *sqlfs)
{
    int r;
    r = break_transaction(sqlfs, 0);
    if (r == SQLITE_BUSY)
        return 2;
    return SQLITE_OK == r;
}


int sqlfs_set_type(sqlfs_t *sqlfs, const char *key, const char *type)
{
    int r = SQLITE_DONE;
    BEGIN
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else
        if (sqlfs_proc_access(sqlfs, key, W_OK | F_OK) != 0)
            r = SQLITE_ERROR;
        else
            r = key_set_type(get_sqlfs(sqlfs), key, type);

    COMPLETE(1)
    if (r == SQLITE_BUSY)
        return 2;
    return (r == SQLITE_DONE);
}



#undef INDEX
#define INDEX 31

int sqlfs_list_keys(sqlfs_t *sqlfs, const char *pattern, void *buf, fuse_fill_dir_t filler)
{
    int r, result = 0;
    const char *tail;
    const char *t;
    static const char *cmd = "select key, mode from meta_data where key glob :pattern; ";
    char tmp[PATH_MAX];
    char *lpath;
    sqlite3_stmt *stmt;
    BEGIN

    lpath = strdup(pattern);
    remove_tail_slash(lpath);

    snprintf(tmp, sizeof(tmp), "%s", pattern);

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

        result = -EACCES;
    }
    if (result == 0)
    {
        sqlite3_bind_text(stmt, 1, tmp, -1, SQLITE_STATIC);

        while (1)
        {
            r = sql_step(stmt);
            if (r == SQLITE_ROW)
            {
                t = sqlite3_column_text(stmt, 0);
                if (filler(buf, t, NULL, 0))
                    break;
            }
            else if (r == SQLITE_DONE)
            {
                break;
            }
            else if (r == SQLITE_BUSY)
            {
                result = -EBUSY;
                break;
            }
            else
            {
                show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
                result = -EACCES;
                break;
            }
        }
        sqlite3_reset(stmt);
    }
    COMPLETE(1)
    free(lpath);
    return result;
}

int sqlfs_is_dir(sqlfs_t *sqlfs, const char *key)
{
    return key_is_dir(sqlfs, key);
}


static int create_db_table(sqlfs_t *sqlfs)
{  /* ensure tables are created if not existing already
                  if already exist, command results ignored so no effects */
    static const char *cmd1 =
        " CREATE TABLE meta_data(key text, type text, inode integer, uid integer, gid integer, mode integer,"
        "acl text, attribute text, atime integer, mtime integer, ctime integer, size integer,"
        "block_size integer, primary key (key), unique(key))" ;
    static const char *cmd2 =
        " CREATE TABLE value_data (key text, block_no integer, data_block blob, unique(key, block_no))";
    static const char *cmd3 = "create index meta_index on meta_data (key);";

    sqlite3_exec(get_sqlfs(sqlfs)->db, cmd1, NULL, NULL, NULL);
    sqlite3_exec(get_sqlfs(sqlfs)->db, cmd2, NULL, NULL, NULL);
    sqlite3_exec(get_sqlfs(sqlfs)->db, cmd3, NULL, NULL, NULL);
    return 1;
}



static void * sqlfs_t_init(const char *db_file)
{
    int i, r;
    sqlfs_t *sql_fs = calloc(1, sizeof(*sql_fs));
    assert(sql_fs);
    for (i = 0; i < (int)(sizeof(sql_fs->stmts) / sizeof(sql_fs->stmts[0])); i++)
    {
        sql_fs->stmts[i] = 0;
    }
    r = sqlite3_open(db_file, &(sql_fs->db));
    if (r != SQLITE_OK)
    {
        fprintf(stderr, "Cannot open the database file %s\n", db_file);
        return 0;
    }

    sql_fs->default_mode = 0700; /* allows the creation of children under / , default user at initialization is 0 (root)*/

    create_db_table( sql_fs);

    if (max_inode == 0)
        max_inode = get_current_max_inode(sql_fs);

    /*sqlite3_busy_timeout( sql_fs->db, 500); *//* default timeout 0.5 seconds */
    sqlite3_exec(sql_fs->db, "PRAGMA synchronous = OFF;", NULL, NULL, NULL);
    ensure_existence(sql_fs, "/", TYPE_DIR);
    return (void *) sql_fs;
}



static void sqlfs_t_finalize(void *arg)
{
    int i;
    sqlfs_t *sql_fs = (sqlfs_t *) arg;
    if (sql_fs)
    {
        for (i = 0; i < (int)(sizeof(sql_fs->stmts) / sizeof(sql_fs->stmts[0])); i++)
        {
            if (sql_fs->stmts[i])
                sqlite3_finalize(sql_fs->stmts[i]);
        }
        sqlite3_close(sql_fs->db);
        free(sql_fs);
    }

}


int sqlfs_open(const char *db_file, sqlfs_t **sqlfs)
{
    if (db_file == 0)
        db_file = default_db_file;
    *sqlfs = sqlfs_t_init(db_file);
    if (!*sqlfs)
        return 0;
    return 1;
}

int sqlfs_close(sqlfs_t *sqlfs)
{

    sqlfs_t_finalize(sqlfs);
    return 1;
}


#ifdef FUSE


static int sqlfs_op_getattr(const char *path, struct stat *stbuf)
{
    return sqlfs_proc_getattr(0, path, stbuf);
}

static int sqlfs_op_access(const char *path, int mask)
{
    return sqlfs_proc_access(0, path, mask);
}
static int sqlfs_op_readlink(const char *path, char *buf, size_t size)
{
    return sqlfs_proc_readlink(0, path, buf, size);
}
static int sqlfs_op_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                            off_t offset, struct fuse_file_info *fi)
{
    return sqlfs_proc_readdir(0, path, buf, filler, offset, fi);
}
static int sqlfs_op_mknod(const char *path, mode_t mode, dev_t rdev)
{
    return sqlfs_proc_mknod(0, path, mode, rdev);
}
static int sqlfs_op_mkdir(const char *path, mode_t mode)
{
    return sqlfs_proc_mkdir(0, path, mode);
}
static int sqlfs_op_unlink(const char *path)
{
    return sqlfs_proc_unlink(0, path);
}
static int sqlfs_op_rmdir(const char *path)
{
    return sqlfs_proc_rmdir(0, path);
}
static int sqlfs_op_symlink(const char *path, const char *to)
{
    return sqlfs_proc_symlink(0, path, to);
}
static int sqlfs_op_rename(const char *from, const char *to)
{
    return sqlfs_proc_rename(0, from, to);
}
static int sqlfs_op_link(const char *from, const char *to)
{
    return sqlfs_proc_link(0, from, to);
}
static int sqlfs_op_chmod(const char *path, mode_t mode)
{
    return sqlfs_proc_chmod(0, path, mode);
}
static int sqlfs_op_chown(const char *path, uid_t uid, gid_t gid)
{
    return sqlfs_proc_chown(0, path, uid, gid);
}
static int sqlfs_op_truncate(const char *path, off_t size)
{
    return sqlfs_proc_truncate(0, path, size);
}
static int sqlfs_op_utime(const char *path, struct utimbuf *buf)
{
    return sqlfs_proc_utime(0, path, buf);
}
int sqlfs_op_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    return sqlfs_proc_create(0, path, mode, fi);
}

static int sqlfs_op_open(const char *path, struct fuse_file_info *fi)
{
    return sqlfs_proc_open(0, path, fi);
}
static int sqlfs_op_read(const char *path, char *buf, size_t size, off_t offset, struct
                         fuse_file_info *fi)
{
    return sqlfs_proc_read(0, path, buf, size, offset, fi);
}
static int sqlfs_op_write(const char *path, const char *buf, size_t size, off_t offset,
                          struct fuse_file_info *fi)
{
    return sqlfs_proc_write(0, path, buf, size, offset, fi);
}
static int sqlfs_op_statfs(const char *path, struct statvfs *stbuf)
{
    return sqlfs_proc_statfs(0, path, stbuf);
}
static int sqlfs_op_release(const char *path, struct fuse_file_info *fi)
{
    return sqlfs_proc_release(0, path, fi);
}
static int sqlfs_op_fsync(const char *path, int isfdatasync, struct fuse_file_info *fi)
{
    return sqlfs_proc_fsync(0, path, isfdatasync, fi);
}
static int sqlfs_op_setxattr(const char *path, const char *name, const char *value,
                             size_t size, int flags)
{
    return sqlfs_proc_setxattr(0, path, name, value, size, flags);
}
static int sqlfs_op_getxattr(const char path, const char *name, char *value, size_t size)
{
    return sqlfs_proc_getxattr(0, path, name, value, size);
}
static int sqlfs_op_listxattr(const char *path, char *list, size_t size)
{
    return sqlfs_proc_listxattr(0, path, list, size);
}
static int sqlfs_op_removexattr(const char *path, const char *name)
{
    return sqlfs_proc_removexattr(0, path, name);
}

static struct fuse_operations sqlfs_op;

#endif

int sqlfs_init(const char *db_file_name)
{
#ifdef FUSE
    sqlfs_op.getattr	= sqlfs_op_getattr;
    sqlfs_op.access	= sqlfs_op_access;
    sqlfs_op.readlink	= sqlfs_op_readlink;
    sqlfs_op.readdir	= sqlfs_op_readdir;
    sqlfs_op.mknod	= sqlfs_op_mknod;
    sqlfs_op.mkdir	= sqlfs_op_mkdir;
    sqlfs_op.symlink	= sqlfs_op_symlink;
    sqlfs_op.unlink	= sqlfs_op_unlink;
    sqlfs_op.rmdir	= sqlfs_op_rmdir;
    sqlfs_op.rename	= sqlfs_op_rename;
    sqlfs_op.link	= sqlfs_op_link;
    sqlfs_op.chmod	= sqlfs_op_chmod;
    sqlfs_op.chown	= sqlfs_op_chown;
    sqlfs_op.truncate	= sqlfs_op_truncate;
    sqlfs_op.utime	= sqlfs_op_utime;
    sqlfs_op.open	= sqlfs_op_open;
    sqlfs_op.create	= sqlfs_op_create;
    sqlfs_op.read	= sqlfs_op_read;
    sqlfs_op.write	= sqlfs_op_write;
    sqlfs_op.statfs	= sqlfs_op_statfs;
    sqlfs_op.release	= sqlfs_op_release;
    sqlfs_op.fsync	= sqlfs_op_fsync;
#if 0

    sqlfs_op.setxattr	= sqlfs_op_setxattr;
    sqlfs_op.getxattr	= sqlfs_op_getxattr;
    sqlfs_op.listxattr	= sqlfs_op_listxattr;
    sqlfs_op.removexattr= sqlfs_op_removexattr;
#endif
#endif

    if (db_file_name)
        strncpy(default_db_file, db_file_name, sizeof(default_db_file));
    pthread_key_create(&sql_key, sqlfs_t_finalize);
    return 0;
}

#ifdef FUSE

int sqlfs_fuse_main(int argc, char **argv)
{


    return fuse_main(argc, argv, &sqlfs_op);
}

#endif
