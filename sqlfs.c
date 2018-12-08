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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#ifdef __ANDROID__
# include <sys/vfs.h>
# define statvfs statfs
# define fstatvfs fstatfs
#else
# include <sys/statvfs.h>
#endif

#ifdef HAVE_LIBSQLCIPHER
# include "sqlcipher/sqlite3.h"
#else
# include "sqlite3.h"
#endif

struct sqlfs_t
{
    sqlite3 *db;
    int transaction_level;
    int in_transaction;
    mode_t default_mode;
    
    sqlite3_stmt *stmts[200];
#ifndef HAVE_LIBFUSE
    uid_t uid;
    gid_t gid;
#endif
};


#define INDEX 0

#define SQLITE3_PREPARE(a, b, c, d, e) \
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
    else \
        r = ~SQLITE_OK; \
    if (r != SQLITE_OK) \
        r = sqlite3_prepare((a), (b), (c), (d), (e)); \
    if (r == SQLITE_OK) \
        get_sqlfs(sqlfs)->stmts[INDEX] = stmt; \
    else \
        get_sqlfs(sqlfs)->stmts[INDEX] = 0;

static const size_t BLOCK_SIZE = 8192;

static pthread_key_t pthread_key;

static int instance_count = 0;

static char default_db_file[PATH_MAX] = { 0 };

/* the key needs to be kept around here for the thread handling, each
 * thread will open a connection to the database on the fly, so each
 * thread needs the key */
static char cached_password[MAX_PASSWORD_LENGTH] = { 0 };

static int max_inode = 0;

static void * sqlfs_t_init(const char *db_file, const char *db_key);
static void sqlfs_t_finalize(void *arg);

static __inline__ int sql_step(sqlite3_stmt *stmt)
{
    int r, i;
    for (i = 0; i < (1 * 1000 / 100); i++)
    {
        r = sqlite3_step(stmt);
        if (r != SQLITE_BUSY)
            break;
    }
    return r;
}

static __inline__ sqlfs_t *get_sqlfs(sqlfs_t *p)
{
    sqlfs_t *sqlfs;

    if (p)
        return p;

    sqlfs = (sqlfs_t *) (pthread_getspecific(pthread_key));
    if (sqlfs)
        return sqlfs;

    sqlfs = (sqlfs_t*) sqlfs_t_init(default_db_file, cached_password);
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

#ifdef __ANDROID__
#include <android/log.h>
#define LOG_TAG "libsqlfs"
#define  LOGW(...)  __android_log_print(ANDROID_LOG_WARN, LOG_TAG, __VA_ARGS__)
static void show_msg(FILE *f, char *fmt, ...)
{
    char buf[1000];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, 1000, fmt, ap);
    va_end(ap);
    __android_log_print(ANDROID_LOG_WARN, LOG_TAG, "%s", buf);
}
#else
static void show_msg(FILE *f, char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);

    vfprintf(f, fmt, ap);
    va_end(ap);
}
#endif /* __ANDROID__ */



void clean_attr(key_attr *attr)
{
    free(attr->path);
    free(attr->type);
    memset(attr, 0, sizeof(*attr));
}

void clean_value(key_value *value)
{
    free(value->data);
    memset(value, 0, sizeof(*value));
}

#undef INDEX
#define INDEX 100

static int begin_transaction(sqlfs_t *sqlfs)
{
    /* begin immediate will immediately obtain a reserved lock on the
     * database but will allow readers to proceed.
    */
    const char *cmd = "begin immediate;";

    sqlite3_stmt *stmt;
    const char *tail;
    int r = SQLITE_OK;

    if (get_sqlfs(sqlfs)->transaction_level == 0)
    {
        int i;
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1,  &stmt,  &tail);
        for (i = 0; i < 10; i++)
        {
            r = sqlite3_step(stmt);
            if (r != SQLITE_BUSY)
                break;
        }
        sqlite3_reset(stmt);
        if (r == SQLITE_DONE)
            r = SQLITE_OK;
        if (r == SQLITE_BUSY)
        {
            show_msg(stderr, "database is busy!\n");
            return r;  /* busy, return back */
        }
        get_sqlfs(sqlfs)->in_transaction = 1;
    }
    get_sqlfs(sqlfs)->transaction_level++;
    return r;
}

#undef INDEX
#define INDEX 101


static int commit_transaction(sqlfs_t *sqlfs, int r0)
{
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

    if ((get_sqlfs(sqlfs)->transaction_level - 1 == 0) && (get_sqlfs(sqlfs)->in_transaction))
    {
        int i;
        for (i = 0; i < 10; i++)
        {
            r = sqlite3_step(stmt);
            if (r != SQLITE_BUSY)
                break;
        }
        sqlite3_reset(stmt);
        if (r == SQLITE_DONE)
            r = SQLITE_OK;
        if (r == SQLITE_BUSY)
        {
            show_msg(stderr, "database is busy!\n");
            return r;  /* busy, return back */
        }
        //**assert(sqlite3_get_autocommit(get_sqlfs(sqlfs)->db) != 0);*/
        get_sqlfs(sqlfs)->in_transaction = 0;
    }
    get_sqlfs(sqlfs)->transaction_level--;

    return r;
}

#undef INDEX
#define INDEX 103

static int break_transaction(sqlfs_t *sqlfs, int r0)
{
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

    if (get_sqlfs(sqlfs)->in_transaction)
    {
        int i;
        for (i = 0; i < 10; i++)
        {
            r = sqlite3_step(stmt);
            if (r != SQLITE_BUSY)
                break;
        }
        sqlite3_reset(stmt);
        if (r == SQLITE_DONE)
            r = SQLITE_OK;
        if (r == SQLITE_BUSY)
        {
            show_msg(stderr, "database is busy!\n");
            return r;  /* busy, return back */
        }
        //**assert(sqlite3_get_autocommit(get_sqlfs(sqlfs)->db) != 0);*/
        get_sqlfs(sqlfs)->in_transaction = 0;
    }

    return r;
}


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
        t = (const char *)sqlite3_column_text(stmt, 0);

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
    begin_transaction(get_sqlfs(sqlfs));
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt, &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
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
            commit_transaction(get_sqlfs(sqlfs), 1);
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
    commit_transaction(get_sqlfs(sqlfs), 1);
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
    begin_transaction(get_sqlfs(sqlfs));
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt, &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
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
            commit_transaction(get_sqlfs(sqlfs), 1);
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
    commit_transaction(get_sqlfs(sqlfs), 1);
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
    begin_transaction(get_sqlfs(sqlfs));
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt, &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
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
            commit_transaction(get_sqlfs(sqlfs), 1);
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
            commit_transaction(get_sqlfs(sqlfs), 1);
            return r;
        }
        sqlite3_bind_text(stmt, 1, n_pattern, -1, SQLITE_STATIC);
        r = sql_step(stmt);
        if (r != SQLITE_ROW)
        {
            if (r == SQLITE_BUSY)
                ;
            else if (r != SQLITE_DONE)
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
    commit_transaction(get_sqlfs(sqlfs), 1);
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
    begin_transaction(get_sqlfs(sqlfs));
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
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
            commit_transaction(get_sqlfs(sqlfs), 1);
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
    commit_transaction(get_sqlfs(sqlfs), 1);
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
    sqlite3_stmt *stmt;

    i = key_is_dir(sqlfs, path);
    if (i == 0)
        return 0;
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
                t = (const char *)sqlite3_column_text(stmt, 0);
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
        sqlite3_reset(stmt);
    }
    free(lpath);

    if (r == SQLITE_BUSY)
        count = -1;
    return count;
}

static int set_attr(sqlfs_t *sqlfs, const char *key, const key_attr *attr);


static int ensure_existence(sqlfs_t *sqlfs, const char *key, const char *type)
{
    if (key_exists(sqlfs, key, 0) == 0)
    {
        int r;
        key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        attr.path = strdup(key);
        attr.type = strdup(type);
        attr.mode = get_sqlfs(sqlfs)->default_mode; /* to use default */
#ifdef HAVE_LIBFUSE
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

    int r;

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
    char tmp[PATH_MAX];
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
    int r;

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
        attr->path = make_str_copy((const char *)sqlite3_column_text(stmt, 0));
        assert(!strcmp(key, attr->path));
        attr->type = make_str_copy((const char *)sqlite3_column_text(stmt, 1));
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
    int r;
    const char *tail;
    sqlite3_stmt *stmt;
    int mode = attr->mode;
    static const char *cmd1 = "insert or ignore into meta_data (key) VALUES ( :key ) ; ";
    static const char *cmd2 = "update meta_data set type = :type, mode = :mode, uid = :uid, gid = :gid,"
                              "atime = :atime, mtime = :mtime, ctime = :ctime,  size = :size, inode = :inode, block_size = :block_size where key = :key; ";

    begin_transaction(get_sqlfs(sqlfs));
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
        commit_transaction(get_sqlfs(sqlfs), 1);
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
        commit_transaction(get_sqlfs(sqlfs), 1);
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
    commit_transaction(get_sqlfs(sqlfs), 1);
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

    begin_transaction(get_sqlfs(sqlfs));
    i = ensure_existence(sqlfs, key, type);
    if (i == 1)
    {
        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            commit_transaction(get_sqlfs(sqlfs), 1);
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
    commit_transaction(get_sqlfs(sqlfs), 1);
    return r;
}



#undef INDEX
#define INDEX 21

/* If the read was successful, SQLITE_OK is returned.  If there is
 * nothing to read, then SQLITE_DONE is returned.  This probably
 * doesn't make sense, but leave it as is for now since it'll be a
 * little project to change it. */
static int get_value_block(sqlfs_t *sqlfs, const char *key, char *data, size_t block_no, size_t *size)
{
    int r;
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
          *size = (size_t) sqlite3_column_bytes(stmt, 0);
        memcpy(data, sqlite3_column_blob(stmt, 0), sqlite3_column_bytes(stmt, 0));
        r = SQLITE_OK;

    }

    sqlite3_reset(stmt);

    return r;
}


#undef INDEX
#define INDEX 22

static int set_value_block(sqlfs_t *sqlfs, const char *key, const char *data, size_t block_no, size_t size)
{
    int r;
    const char *tail;
    sqlite3_stmt *stmt;

    static const char *cmd = "update value_data set data_block = :data_block where key = :key and block_no = :block_no;";
    static const char *cmd1 = "insert or ignore into value_data (key, block_no) VALUES ( :key, :block_no ) ; ";
    static const char *cmd2 = "delete from value_data  where key = :key and block_no = :block_no;";

    begin_transaction(get_sqlfs(sqlfs));

    if (size == 0)
    {

        SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd2, -1, &stmt,  &tail);
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            commit_transaction(get_sqlfs(sqlfs), 1);
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
        commit_transaction(get_sqlfs(sqlfs), 1);
        return r;
    }


#undef INDEX
#define INDEX 23


    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd1, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, block_no);
    r = sql_step(stmt);
    sqlite3_reset(stmt);

    if (r == SQLITE_BUSY)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return r;
    }

#undef INDEX
#define INDEX 24


    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
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

    commit_transaction(get_sqlfs(sqlfs), 1);
    return r;
}



#undef INDEX
#define INDEX 25


static int get_value(sqlfs_t *sqlfs, const char *key, key_value *value, size_t begin, size_t end)
{
    int r;
    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd = "select size from meta_data where key = :key; ";

    begin_transaction(get_sqlfs(sqlfs));

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
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
        size_t filesize = sqlite3_column_int64(stmt, 0);
        if ((end == 0) || (end > filesize))
            end = filesize;
        r = SQLITE_OK;
    }

    if (r == SQLITE_OK)
    {
        if (begin < end)
        {
            size_t block_no = begin / BLOCK_SIZE;
            size_t blockbegin = block_no * BLOCK_SIZE; // rounded down to nearest block
            size_t blockend = end / BLOCK_SIZE * BLOCK_SIZE; // beginning of last block
            size_t offset = begin - blockbegin;
            char *block = calloc(BLOCK_SIZE, sizeof(char));
            char *data = value->data; // pointer to move along as it is written to
            assert(value->data);
            { /* handle first block, whether it is the whole block, or only part of it */
                size_t readsize = BLOCK_SIZE - offset;
                if (value->size < readsize)
                  readsize = value->size;
                r = get_value_block(sqlfs, key, block, block_no, NULL);
                memcpy(data, block + offset, readsize);
                block_no++;
                blockbegin += BLOCK_SIZE;
                data += readsize;
            }
            /* read complete blocks in the middle of the write */
            while ((r == SQLITE_OK) && (blockbegin < blockend))
            {
                r = get_value_block(sqlfs, key, data, block_no, NULL);
                if (r != SQLITE_OK)
                    break;
                block_no++;
                blockbegin += BLOCK_SIZE;
                data += BLOCK_SIZE;
            }
            /* partial block at the end of the read */
            if ((r == SQLITE_OK) && (blockbegin < end))
            {
                assert(blockbegin % BLOCK_SIZE == 0);
                assert(end - blockbegin < BLOCK_SIZE);
                r = get_value_block(sqlfs, key, block, block_no, NULL);
                memcpy(data, block, end - blockend);
            }
            free(block);
        }
        else
        {
            r = SQLITE_NOTFOUND;
        }
    }

    sqlite3_reset(stmt);
    key_accessed(sqlfs, key);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return r;
}


#undef INDEX
#define INDEX 26

/* 'begin' and 'end' are the positions in bytes relative to the file
 * to start and finish writing to. */
static int set_value(sqlfs_t *sqlfs, const char *key, const key_value *value, size_t begin, size_t end)
{
    int r;
    const char *tail;
    sqlite3_stmt *stmt;
    size_t current_file_size = 0;
    static const char *selectsize = "select size from meta_data where key = :key ";
    static const char *createfile_cmd = "insert or ignore into meta_data (key) VALUES ( :key ) ; ";
    static const char *updatesize_cmd = "update meta_data set size = :size where key =  :key  ; ";

    /* get the size of the file if it already exists */
    r = sqlite3_prepare(get_sqlfs(sqlfs)->db, selectsize, -1, &stmt, &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    if (r != SQLITE_OK)
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
    r = sql_step(stmt);
    if (r != SQLITE_ROW)
    {
        if (r != SQLITE_DONE)
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        if (r == SQLITE_BUSY)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            return r;
        }
    }
    else
        current_file_size = sqlite3_column_int64(stmt, 0);
    sqlite3_reset(stmt);


    begin_transaction(get_sqlfs(sqlfs));
    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, createfile_cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
        return r;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    sqlite3_reset(stmt);

    if (r == SQLITE_BUSY)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
        return r;
    }

#undef INDEX
#define INDEX 27


    {
        size_t block_no;
        size_t blockbegin, blockend, length, position_in_value = 0;
        char tmp[BLOCK_SIZE];

        if (end == 0)
            end = begin + value->size;
        block_no = begin / BLOCK_SIZE;
        blockbegin = block_no * BLOCK_SIZE; // 'begin' chopped to BLOCK_SIZE increments
        // beginning of last block, i.e. 'end' rounded to 'BLOCK_SIZE'
        blockend = end / BLOCK_SIZE * BLOCK_SIZE;

        /* partial write in the first block */
        {
            size_t end_of_this_block, old_size = 0;

            r = get_value_block(sqlfs, key, tmp, block_no, &old_size);
            /* SQLITE_OK == read data, SQLITE_DONE == no data */
            if (r != SQLITE_OK && r != SQLITE_DONE)
            {
                show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
                commit_transaction(get_sqlfs(sqlfs), 1);
                return r;
            }
            if (end > blockbegin + BLOCK_SIZE)
                // the write spans multiple blocks, only write first one
                end_of_this_block = blockbegin + BLOCK_SIZE;
            else
                end_of_this_block = end; // the write fits in a single block
            position_in_value = end_of_this_block - begin;

            memcpy(tmp + (begin - blockbegin), value->data, position_in_value);
            length = end_of_this_block - blockbegin;
            if (length < old_size)
                length = old_size;
            r = set_value_block(sqlfs, key, tmp, block_no, length);
            block_no++;
            blockbegin += BLOCK_SIZE;
        }

        /* writing complete blocks in the middle of the write */
        while ((r == SQLITE_OK) && (blockbegin < blockend))
        {
            r = set_value_block(sqlfs, key, value->data + position_in_value, block_no, BLOCK_SIZE);
            block_no++;
            blockbegin += BLOCK_SIZE;
            position_in_value += BLOCK_SIZE;
        }
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
            commit_transaction(get_sqlfs(sqlfs), 1);
            return r;
        }

        /* partial block at the end of the write */
        if (blockbegin < end)
        {
            size_t get_value_size;

            assert(blockbegin % BLOCK_SIZE == 0);
            assert(end - blockbegin < (size_t) BLOCK_SIZE);

            memset(tmp, 0, BLOCK_SIZE);
            r = get_value_block(sqlfs, key, tmp, block_no, &get_value_size);
            if (r != SQLITE_OK)
                get_value_size = 0;
            memcpy(tmp, value->data + position_in_value, end - blockbegin);
            if (get_value_size < (end - blockbegin))
                get_value_size = end - blockbegin;

            r = set_value_block(sqlfs, key, tmp, block_no, get_value_size);
        }
    }

    SQLITE3_PREPARE(get_sqlfs(sqlfs)->db, updatesize_cmd, -1, &stmt,  &tail);
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));
        commit_transaction(get_sqlfs(sqlfs), 1);
        return r;
    }
    sqlite3_bind_int64(stmt, 1, (end > current_file_size) ? end : current_file_size);
    sqlite3_bind_text(stmt, 2, key, -1, SQLITE_STATIC);
    r = sql_step(stmt);
    sqlite3_reset(stmt);
    if (r == SQLITE_DONE)
        r = SQLITE_OK;
    key_modified(sqlfs, key);
    /*ensure_parent_existence(sqlfs, key);*/
    commit_transaction(get_sqlfs(sqlfs), 1);
    return r;
}


#undef INDEX
#define INDEX 28


static int key_shorten_value(sqlfs_t *sqlfs, const char *key, size_t new_length)
{
    int r;
    size_t l, i, block_no;
    char *tmp;
    const char *tail;
    sqlite3_stmt *stmt;
    static const char *cmd1 = "delete from value_data where key = :key and block_no > :block_no; ";
    static const char *cmd2 = "update meta_data set size = :size where key =  :key  ; ";

    begin_transaction(get_sqlfs(sqlfs));
    i = key_exists(sqlfs, key, &l);
    if (i == 0)
    {
        assert(0);
        show_msg(stderr, "Illegal truncateion on non-existence key %s\n", key);
        commit_transaction(get_sqlfs(sqlfs), 1);
        return SQLITE_ERROR;
    }
    else if (i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
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
        show_msg(stderr, "%s\n", sqlite3_errmsg(get_sqlfs(sqlfs)->db));

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
    commit_transaction(get_sqlfs(sqlfs), 1);
    return r;
}

static int check_parent_access(sqlfs_t *sqlfs, const char *path)
{
    char ppath[PATH_MAX];
    int r, result = 0;

    begin_transaction(get_sqlfs(sqlfs));
    r = get_parent_path(path, ppath);
    if (r == SQLITE_OK)
    {
        result = check_parent_access(sqlfs, ppath);
        if (result == 0)
            result = (sqlfs_proc_access(sqlfs, (ppath), X_OK));
    }
    /* else if no parent, we return 0 by default */

    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}


static int check_parent_write(sqlfs_t *sqlfs, const char *path)
{
    char ppath[PATH_MAX];

    int r, result = 0;

    begin_transaction(get_sqlfs(sqlfs));
    r = get_parent_path(path, ppath);
    if (r == SQLITE_OK)
    {
        result = (sqlfs_proc_access(sqlfs, (ppath), W_OK | X_OK));
#ifndef HAVE_LIBFUSE
        /* libfuse seems to enforce that the parent directory before getting
         * here, but without libfuse, we need to do it manually */
        if (result == -ENOENT)
        {
            result = check_parent_write(sqlfs, ppath);
            if (result == 0)
                ensure_existence(sqlfs, ppath, TYPE_DIR);
            result = (sqlfs_proc_access(sqlfs, (ppath), W_OK | X_OK));
        }
#endif
    }
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

#define CHECK_PARENT_PATH(p)                     \
    result = check_parent_access(sqlfs, (p));    \
    if (result != 0)                             \
    {                                            \
        commit_transaction(get_sqlfs(sqlfs), 1); \
        return result;                           \
    }

#define CHECK_READ(p)                                       \
    result = (sqlfs_proc_access(sqlfs, (p), R_OK | F_OK));  \
    if (result != 0)                                        \
    {                                                       \
        commit_transaction(get_sqlfs(sqlfs), 1);            \
        return result;                                      \
    }

#define CHECK_WRITE(p)                                      \
    result = (sqlfs_proc_access(sqlfs, (p), W_OK | F_OK));  \
    if (result != 0)                                        \
    {                                                       \
        commit_transaction(get_sqlfs(sqlfs), 1);            \
        return result;                                      \
    }

#define CHECK_DIR_WRITE(p)                                          \
    result = (sqlfs_proc_access(sqlfs, (p), W_OK | F_OK | X_OK));   \
    if (result != 0)                                                \
    {                                                               \
        commit_transaction(get_sqlfs(sqlfs), 1);                    \
        return result;                                              \
    }

#define CHECK_DIR_READ(p)                                           \
    result = (sqlfs_proc_access(sqlfs, (p), R_OK | F_OK | X_OK));   \
    if (result != 0)                                                \
    {                                                               \
        show_msg(stderr, "dir read failed %d\n", result);           \
        commit_transaction(get_sqlfs(sqlfs), 1);                    \
        return result;                                              \
    }

#define CHECK_PARENT_READ(p)                                            \
    {                                                                   \
        char ppath[PATH_MAX];                                           \
        if (SQLITE_OK == get_parent_path((p), ppath))                   \
        {                                                               \
            result = (sqlfs_proc_access(sqlfs, (ppath), R_OK | X_OK));  \
            if (result != 0)                                            \
            {                                                           \
                commit_transaction(get_sqlfs(sqlfs), 1);                \
                return result;                                          \
            }                                                           \
        }                                                               \
    }

#define CHECK_PARENT_WRITE(p)                           \
    {                                                   \
        result = check_parent_write(sqlfs, (p));        \
        if (result != 0)                                \
        {                                               \
            commit_transaction(get_sqlfs(sqlfs), 1);    \
            return result;                              \
        }                                               \
    }

int sqlfs_proc_getattr(sqlfs_t *sqlfs, const char *path, struct stat *stbuf)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    int r, result = 0;

    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);
    CHECK_READ(path);

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
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

static int gid_in_supp_groups(gid_t gid)
{
    int num_groups = getgroups(0, 0);
    int r = 0;
    if (num_groups)
    {
        gid_t *gids = malloc(sizeof(gids[0]) * num_groups);
        int n = getgroups(num_groups, gids);

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

    int r = SQLITE_OK, result = 0;
#ifdef HAVE_LIBFUSE
    gid_t gid = getegid();
    uid_t uid = geteuid();
#else
    gid_t gid = get_sqlfs(sqlfs)->gid;
    uid_t uid = get_sqlfs(sqlfs)->uid;
#endif
    /* init based on least permission, in case of trouble */
    uid_t fuid = UINT_MAX;
    gid_t fgid = UINT_MAX;
    mode_t fmode = 0;

    begin_transaction(get_sqlfs(sqlfs));

    if (uid == 0) /* root user so everything is granted */
    {
        int i = key_exists(sqlfs, path, 0);
        if (i == 0)
            result = -ENOENT;
        else if (i == 2)
            result = -EBUSY;

        commit_transaction(get_sqlfs(sqlfs), 1);
        return result;
    }

    if (mask & F_OK)
    {
        r = get_parent_permission_data(sqlfs, path, &fgid, &fuid, &fmode);
        if (r == SQLITE_OK)
        {
            if (uid == (uid_t) fuid)
            {
                if (!(S_IRUSR  & S_IXUSR & fmode))
                    result = -EACCES;
            }
            else if ((gid == (gid_t) fgid) || (gid_in_supp_groups(fgid)))
            {
                if (!(S_IRGRP  & S_IXGRP & fmode))
                    result = -EACCES;
            }
            else
            {
                if (!(S_IROTH  & S_IXOTH & fmode))
                    result = -EACCES;
            }
        }
        else if (r == SQLITE_NOTFOUND)
            result = -ENOENT;
    }

    if (result == 0)
        r = get_permission_data(get_sqlfs(sqlfs), path, &fgid, &fuid, &fmode);
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

    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_readlink(sqlfs_t *sqlfs, const char *path, char *buf, size_t size)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    key_value value = { 0, 0 };
    int r, result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);
    CHECK_READ(path);
    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK)
    {
        if (!strcmp(attr.type, TYPE_SYM_LINK))
        {
            value.data = buf;
            value.size = size;
            r = get_value(get_sqlfs(sqlfs), path, &value, 0, size);
            if (r == SQLITE_OK)
            {
                if (attr.size > size)
                    show_msg(stderr,
                             "warning: readlink provided buffer too small\n");
                strncpy(buf, value.data, size);
            }
        }
        else
        {
            result = -EINVAL;
        }
    }
    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
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
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);
    CHECK_DIR_READ(path);

    i = key_is_dir(get_sqlfs(sqlfs), path);
    if (i == 0)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -ENOTDIR;
    }
    else if (i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
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
                t = (const char *)sqlite3_column_text(stmt, 0);
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
    commit_transaction(get_sqlfs(sqlfs), 1);
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
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(path);

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK)
    {
        clean_attr(&attr);
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EEXIST;
    }
    attr.path = strdup(path);
    attr.type = strdup(TYPE_BLOB);
    attr.mode = mode;
#ifdef HAVE_LIBFUSE
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
        result =  -EINVAL;
    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_mkdir(sqlfs_t *sqlfs, const char *path, mode_t mode)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    int r, result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(path);

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK)
    {
        clean_attr(&attr);
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EEXIST;
    }
    attr.path = strdup(path);
    attr.type = strdup(TYPE_DIR);
    attr.mode = mode;

#ifdef HAVE_LIBFUSE
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
        result = -EINVAL;
    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_unlink(sqlfs_t *sqlfs, const char *path)
{
    int i, result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(path);

    i = key_exists(get_sqlfs(sqlfs), path, 0);
    if (i == 0)
        result = -ENOENT;
    else if (i == 2)
        result = -EBUSY;

    if (key_is_dir(get_sqlfs(sqlfs), path))
        result = -EISDIR;

    if (result == 0)
    {
        int r = remove_key(get_sqlfs(sqlfs), path);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
            result = -EIO;
    }
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_rmdir(sqlfs_t *sqlfs, const char *path)
{
    int result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(path);

    if (get_dir_children_num(get_sqlfs(sqlfs), path) > 0)
    {
        result = -ENOTEMPTY;
    }
    else
    {
        int r = remove_key(get_sqlfs(sqlfs), path);
        if (r != SQLITE_OK)
            result = -EIO;
    }
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_symlink(sqlfs_t *sqlfs, const char *path, const char *to)
{
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    key_value value = { 0, 0 };
    int r, result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(to);

    r = get_attr(get_sqlfs(sqlfs), to, &attr);
    if (r == SQLITE_OK)
    {
        clean_attr(&attr);
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EEXIST;
    }

    attr.path = strdup(to);
    attr.type = strdup(TYPE_SYM_LINK);
    attr.mode = get_sqlfs(sqlfs)->default_mode; /* 0777 ?? */
#ifdef HAVE_LIBFUSE
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
        commit_transaction(get_sqlfs(sqlfs), 1);
        if (r == SQLITE_BUSY)
            return -EBUSY;
        return -EINVAL;
    }
    clean_attr(&attr);
    value.data = strdup(path);
    value.size = strlen(value.data) + 1;

    r = set_value(get_sqlfs(sqlfs), to, &value, 0, 0);
    if (r != SQLITE_OK)
        result = -EIO;

    clean_value(&value);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

static int rename_dir_children(sqlfs_t *sqlfs, const char *old, const char *new)
{
    int i, r, result = 0;
    const char *tail;
    const char *child_path, *child_filename;
    static const char *cmd = "select key, mode from meta_data where key glob :pattern; ";
    char tmp[PATH_MAX];
    char *lpath, *rpath;
    sqlite3_stmt *stmt;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(old);
    CHECK_DIR_READ(old);

    i = key_is_dir(get_sqlfs(sqlfs), old);
    if (i == 0)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -ENOTDIR;
    }
    else if (i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EBUSY;
    }

    lpath = strdup(old);
    remove_tail_slash(lpath);
    rpath = strdup(new);
    remove_tail_slash(rpath);
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
                child_path = (const char *)sqlite3_column_text(stmt, 0);
                if (!strcmp(child_path, lpath))
                    continue;
                child_filename = child_path + strlen(lpath) + 1;
                if (*child_filename == 0) /* special case when dir the root directory */
                    continue;

                char new_path[PATH_MAX];
                strncpy(new_path, rpath, PATH_MAX);
                new_path[PATH_MAX-2] = 0; // make sure there is a terminating null and room for "/"
                strncat(new_path, "/", 1);
                strncat(new_path, child_filename, PATH_MAX - strlen(new_path) - 1);

                i = key_exists(get_sqlfs(sqlfs), new_path, 0);
                if (i == 1)
                {
                    r = remove_key(get_sqlfs(sqlfs), new_path);
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
                    r = rename_key(get_sqlfs(sqlfs), child_path, new_path);
                    if (r != SQLITE_OK)
                    {
                        result = -EIO;
                        if (r == SQLITE_BUSY)
                            result = -EBUSY;
                    }
                }
            }
            else if (r == SQLITE_DONE)
                break;
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
    commit_transaction(get_sqlfs(sqlfs), 1);
    free(lpath);
    free(rpath);
    return result;
}

int sqlfs_proc_rename(sqlfs_t *sqlfs, const char *from, const char *to)
{
    int i, r = SQLITE_OK, result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(from);
    CHECK_PARENT_WRITE(to);

    i = key_exists(get_sqlfs(sqlfs), from, 0);
    if (i == 0)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EIO;
    }
    else if (i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EBUSY;
    }

    if (key_is_dir(get_sqlfs(sqlfs), to) == 1)
    {
        if (!key_is_dir(get_sqlfs(sqlfs), from))
            result = -EISDIR;
    }
    /* "'from' can specify a directory.  In this case, 'to' must either not exist,
     * or it must specify an empty directory" - (man 2 rename.)
     */
    if ((result == 0) && (key_is_dir(get_sqlfs(sqlfs), from) == 1))
    {
        if (key_exists(get_sqlfs(sqlfs), to, 0))
        {
            if (!key_is_dir(get_sqlfs(sqlfs), to))
                result = -ENOTDIR;
            else if (get_dir_children_num(get_sqlfs(sqlfs), to) >= 0)
                result = -ENOTEMPTY;
        }
        if (result == 0)
            result = rename_dir_children(get_sqlfs(sqlfs), from, to);
    }

    i = key_exists(get_sqlfs(sqlfs), to, 0);
    if (i == 1)
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
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_link(sqlfs_t *sqlfs, const char *from, const char *to)
{
    /* hard link not supported, not allowed */
    return - EACCES;
}

int sqlfs_proc_chmod(sqlfs_t *sqlfs, const char *path, mode_t mode)
{
    int r, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r != SQLITE_OK)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        clean_attr(&attr);
        if (r == SQLITE_BUSY)
            return -EBUSY;
        return -ENOENT;
    }
#ifdef HAVE_LIBFUSE
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
            result = -EACCES;
    }

    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_chown(sqlfs_t *sqlfs, const char *path, uid_t uid, gid_t gid)
{
    int r, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ;

    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r != SQLITE_OK)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        clean_attr(&attr);
        if (r == SQLITE_BUSY)
            return -EBUSY;
        return -ENOENT;
    }
#ifdef HAVE_LIBFUSE
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
            result = -EACCES;
    }
    else
    {
        result = -EACCES;
    }
    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_truncate(sqlfs_t *sqlfs, const char *path, off_t size)
{
    int i, r, result = 0;
    size_t existing_size = 0;
    key_value value = { 0, 0 };

    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);
    CHECK_WRITE(path);

    i = key_exists(sqlfs, path, &existing_size);
    if (i == 0)
        result = -ENOENT;
    else if (i == 2)
        result = -EBUSY;
    if (i == 0 || i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return result;
    }

    if (existing_size > (size_t) size)
    {
        value.size = size;
        r = key_shorten_value(get_sqlfs(sqlfs), path, value.size);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
            result = -EIO;
    }
    else if (existing_size < (size_t) size)
    {
        value.size = size - existing_size;
        value.data = calloc(value.size, sizeof(char));
        memset(value.data, 0, value.size);
        r = set_value(get_sqlfs(sqlfs), path, &value, existing_size, size);
        if (r != SQLITE_OK)
        {
            if (r == SQLITE_BUSY)
                result = -EBUSY;
            else
                result = -EACCES;
        }
    }
    clean_value(&value);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_utime(sqlfs_t *sqlfs, const char *path, struct utimbuf *buf)
{
    int r, result = 0;
    time_t now;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);
    CHECK_WRITE(path);
    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r != SQLITE_OK)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
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
        else
            result = -EACCES;
    }

    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_create(sqlfs_t *sqlfs, const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int r, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    if (fi->direct_io)
        return  -EACCES;

    fi->flags |= O_CREAT | O_WRONLY | O_TRUNC;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(path);

    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK) /* already exists */
    {
        if ((fi->flags & O_EXCL) && (fi->flags & O_CREAT))
        {

            result = -EEXIST;
        }
        else if (!strcmp(attr.type, TYPE_DIR) && (fi->flags & (O_WRONLY | O_RDWR)))
        {

            result = -EISDIR;
        }
    }
    else if (r == SQLITE_BUSY)
        result = -EBUSY;
    else
        /* does not exist */
        if ((fi->flags & O_CREAT) == 0)
            result = - ENOENT ;
    if (result == 0)
    {
        attr.mode = mode;
#ifdef HAVE_LIBFUSE
        attr.uid = geteuid();
        attr.gid = getegid();
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
        else if (r != SQLITE_OK)
            result = -EACCES;
    }
    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_open(sqlfs_t *sqlfs, const char *path, struct fuse_file_info *fi)
{
    int r, exists = 0, result = 0;
    key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    if (fi->direct_io)
        return  -EACCES;
    begin_transaction(get_sqlfs(sqlfs));

    if ((fi->flags & O_CREAT) )
    {
        CHECK_PARENT_WRITE(path);
    }
    else if (fi->flags & (O_WRONLY | O_RDWR))
    {
        CHECK_PARENT_PATH(path);
        CHECK_WRITE(path);
    }
    else
    {
        CHECK_PARENT_PATH(path);
        CHECK_READ(path);
    }
    r = get_attr(get_sqlfs(sqlfs), path, &attr);
    if (r == SQLITE_OK) /* already exists */
    {
        exists = 1;
        if ((fi->flags & O_EXCL) && (fi->flags & O_CREAT) )
            result = -EEXIST;
        else if (!strcmp(attr.type, TYPE_DIR) && (fi->flags & (O_WRONLY | O_RDWR)))
            result = -EISDIR;
    }
    else if (r == SQLITE_BUSY)
        result = -EBUSY;
    else
        /* does not exist */
        if ((fi->flags & O_CREAT) == 0)
            result = - ENOENT ;

    /* truncate file if called for */
    if (exists && (fi->flags & O_TRUNC) && (fi->flags & (O_WRONLY | O_RDWR))) {
        r = sqlfs_proc_truncate(sqlfs, path, 0);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
            result = -EIO;
        /* we need to refresh the attr struct because we've
         * invalidated the old one by truncating */
        r = get_attr(get_sqlfs(sqlfs), path, &attr);
        if (r == SQLITE_BUSY)
            result = -EBUSY;
        else if (r != SQLITE_OK)
            result = -EIO;
    }

    if (!exists && (result == 0) && (fi->flags & O_CREAT))
    {
        attr.mode = get_sqlfs(sqlfs)->default_mode; /* to use some kind of default */
#ifdef HAVE_LIBFUSE
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
        else if (r != SQLITE_OK)
            result = -EACCES;
    }
    clean_attr(&attr);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_read(sqlfs_t *sqlfs, const char *path, char *buf, size_t size, off_t offset, struct
                    fuse_file_info *fi)
{
    int i, r, result = 0;
    key_value value = { 0, 0 };
    size_t existing_size = 0;

    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_PATH(path);
    CHECK_READ(path);

    i = key_is_dir(get_sqlfs(sqlfs), path);
    if (i == 1)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EISDIR;
    }
    else if (i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EBUSY;
    }

    /*if (fi)
    if ((fi->flags & (O_RDONLY | O_RDWR)) == 0)
        return - EBADF;*/

    i = key_exists(get_sqlfs(sqlfs), path, &existing_size);
    if (i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EBUSY;
    }

    if ((size_t) offset >= existing_size) /* nothing to read */
    {
        result = 0;
    }
    else
    {
        value.data = buf;
        value.size = size;
        r = get_value(get_sqlfs(sqlfs), path, &value, offset, offset + size);
        if (r != SQLITE_OK) {
            result = -EIO;
        } else if ((size_t) offset + size > existing_size) /* can read less than asked for */
            result = existing_size - offset;
        else
            result = size;
    }

    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

int sqlfs_proc_write(sqlfs_t *sqlfs, const char *path, const char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
    int i, r, result = 0;
    size_t existing_size = 0;
    key_value value = { 0, 0 };

    begin_transaction(get_sqlfs(sqlfs));

    i = key_is_dir(get_sqlfs(sqlfs), path);
    if (i == 1)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EISDIR;
    }
    else if (i == 2)
    {
        commit_transaction(get_sqlfs(sqlfs), 1);
        return -EBUSY;
    }

    /*if (fi)
    if ((fi->flags & (O_WRONLY | O_RDWR)) == 0)
        return - EBADF;*/

    i = key_exists(get_sqlfs(sqlfs), path, &existing_size);
    if (i == 0)
    { // path to write to does not exist
        key_attr attr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        CHECK_PARENT_WRITE(path);
        attr.path = strdup(path);
        attr.type = strdup(TYPE_BLOB);
        attr.mode = get_sqlfs(sqlfs)->default_mode; /* use default mode */
#ifdef HAVE_LIBFUSE
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
    { // path to write to already exists
        CHECK_PARENT_PATH(path);
        CHECK_WRITE(path);
    }

    if (result == 0)
    {
        size_t write_begin, write_end;
        if (fi && (fi->flags & O_APPEND))
        {/* handle O_APPEND'ing to an existing file. When O_APPEND is set,
            ignore offset, since that's what POSIX does in a similar situation.
            For more info: https://dev.guardianproject.info/issues/250 */
            value.size = size;
            value.data = (char*) buf;
            write_begin = existing_size;
            write_end = existing_size + size;
        }
        else if ((size_t) offset > existing_size)
        { /* handle writes that start after the end of the existing data.  'buf'
             cannot be used directly with set_value() because the buffer given
             to set_value() needs to include any empty space between the end of
             the existing file and the offset. The return value needs to then be
             set to the number of bytes of _data_ written, not the total number
             of bytes written, which would also include that empty space. */
            value.size = offset - existing_size + size;
            value.data = calloc(value.size, sizeof(char));
            memset(value.data, 0, offset - existing_size);
            memcpy(value.data + (offset - existing_size), buf, size);
            write_begin = existing_size;
            write_end = size + offset;
        }
        else
        {
            value.size = size;
            value.data = (char*) buf;
            write_begin = offset;
            write_end = size + offset;
        }
        r = set_value(get_sqlfs(sqlfs), path, &value, write_begin, write_end);
        if (r != SQLITE_OK)
        {
            result = -EIO;
        }
        else if ((size_t) offset > existing_size)
        {
          /* this is the only case that uses calloc(), so
             clean_value() should only be run here, otherwise it will
             free() 'buf', which has been handed in by the caller. */
            clean_value(&value);
            // blank space was filled in, but there was only 'size' data
            result = size;
        }
        else
        {
            result = value.size;
        }
    }
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}

/* we are faking this somewhat by using the data from the underlying
 partition that the database file is stored on. That means we ignore
 the path passed in and just use the default_db_name. */
int sqlfs_proc_statfs(sqlfs_t *sqlfs, const char *path, struct statvfs *stbuf)
{
#ifdef __ANDROID__
    struct statfs sb;
    int rc = TEMP_FAILURE_RETRY(statfs(default_db_file, &sb));
    if (rc == -1) {
        return -errno;
    }
    stbuf->f_namelen = sb.f_namelen;
#else
    struct statvfs sb;
    int rc = statvfs(default_db_file, &sb);
    if (rc == -1) {
        return -errno;
    }
    stbuf->f_namemax = sb.f_namemax;
    stbuf->f_flag = sb.f_flag | ST_NOSUID; // TODO set S_RDONLY based on perms of file
    /* TODO implement the inode info using information from the db itself */
    stbuf->f_favail = 99;
#endif
    /* TODO implement the inode info using information from the db itself */
    stbuf->f_ffree = 99;
    stbuf->f_files = 999;
    /* some guesses at how things should be represented */
    stbuf->f_frsize = BLOCK_SIZE;
    stbuf->f_bsize = sb.f_bsize;
    stbuf->f_bfree = sb.f_bfree;

    struct stat st;
    rc = stat(default_db_file, &st);
    if (rc == -1) {
        return -errno;
    }
    sb.f_blocks = st.st_blocks * sb.f_bsize / stbuf->f_frsize;
    return 0;
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

/* xattr operations are optional and can safely be left unimplemented
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
*/

int sqlfs_del_tree(sqlfs_t *sqlfs, const char *key)
{
    int i, result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(key);
    CHECK_DIR_WRITE(key);

    i = key_exists(get_sqlfs(sqlfs), key, 0);
    if (i == 0)
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
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}


int sqlfs_del_tree_with_exclusion(sqlfs_t *sqlfs, const char *key, const char *exclusion_pattern)
{
    int i, result = 0;
    begin_transaction(get_sqlfs(sqlfs));
    CHECK_PARENT_WRITE(key);
    CHECK_DIR_WRITE(key);

    i = key_exists(get_sqlfs(sqlfs), key, 0);
    if (i == 0)
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
    commit_transaction(get_sqlfs(sqlfs), 1);
    return result;
}



int sqlfs_get_value(sqlfs_t *sqlfs, const char *key, key_value *value,
                    size_t begin, size_t end)
{
    int r = SQLITE_OK;
    begin_transaction(get_sqlfs(sqlfs));
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else if (sqlfs_proc_access(sqlfs, key, R_OK | F_OK) != 0)
        r = SQLITE_ERROR;
    else
        r = get_value(get_sqlfs(sqlfs), key, value, begin, end);

    commit_transaction(get_sqlfs(sqlfs), 1);
    if (r == SQLITE_NOTFOUND)
        return -1;
    return SQLITE_OK == r;
}

int sqlfs_set_value(sqlfs_t *sqlfs, const char *key, const key_value *value,
                    size_t begin,  size_t end)
{
    int r = SQLITE_OK;
    begin_transaction(get_sqlfs(sqlfs));
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else if (sqlfs_proc_access(sqlfs, key, W_OK | F_OK) != 0)
        r = SQLITE_ERROR;
    else
        r = set_value(get_sqlfs(sqlfs), key, value, begin, end);
    commit_transaction(get_sqlfs(sqlfs), 1);
    return SQLITE_OK == r;
}

int sqlfs_get_attr(sqlfs_t *sqlfs, const char *key, key_attr *attr)
{
    int i, r = 1;
    begin_transaction(get_sqlfs(sqlfs));
    if ((i = check_parent_access(sqlfs, key)) != 0)
    {
        if (i == -ENOENT)
            r = -1;
        else if (i == -EACCES)
            r = -2;
        else r = -1;
    }
    else if ((i = sqlfs_proc_access(sqlfs, key, R_OK | F_OK)) != 0)
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
    commit_transaction(get_sqlfs(sqlfs), 1);
    return r;
}

int sqlfs_set_attr(sqlfs_t *sqlfs, const char *key, const key_attr *attr)
{
    int r = SQLITE_OK;

    begin_transaction(get_sqlfs(sqlfs));
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else if (sqlfs_proc_access(sqlfs, key, W_OK | F_OK) != 0)
        r = SQLITE_ERROR;
    else
        r = set_attr(get_sqlfs(sqlfs), key, attr);

    commit_transaction(get_sqlfs(sqlfs), 1);
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
    begin_transaction(get_sqlfs(sqlfs));
    if (check_parent_access(sqlfs, key) != 0)
        r = SQLITE_ERROR;
    else if (sqlfs_proc_access(sqlfs, key, W_OK | F_OK) != 0)
        r = SQLITE_ERROR;
    else
        r = key_set_type(get_sqlfs(sqlfs), key, type);

    commit_transaction(get_sqlfs(sqlfs), 1);
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
    begin_transaction(get_sqlfs(sqlfs));

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
                t = (const char *)sqlite3_column_text(stmt, 0);
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
    commit_transaction(get_sqlfs(sqlfs), 1);
    free(lpath);
    return result;
}

int sqlfs_is_dir(sqlfs_t *sqlfs, const char *key)
{
    return key_is_dir(sqlfs, key);
}

static int create_db_table(sqlfs_t *sqlfs)
{
    /* ensure tables are created if not existing already
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

static void * sqlfs_t_init(const char *db_file, const char *password)
{
    int i, r;
    sqlfs_t *sql_fs = calloc(1, sizeof(*sql_fs));
    assert(sql_fs);
    for (i = 0; i < (int)(sizeof(sql_fs->stmts) / sizeof(sql_fs->stmts[0])); i++)
    {
        sql_fs->stmts[i] = 0;
    }
    if (db_file && db_file[0] == 0)
        show_msg(stderr, "WARNING: blank db file name! Creating temporary database.\n");
    r = sqlite3_open(db_file, &(sql_fs->db));
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "Cannot open the database file %s\n", db_file);
        return 0;
    }

#ifdef HAVE_LIBSQLCIPHER
    if (password && strlen(password))
    {
        r = sqlite3_key(sql_fs->db, password, strlen(password));
        if (r != SQLITE_OK)
        {
            show_msg(stderr, "Opening the database with provided key/password failed!\n");
            return 0;
        }
        sqlite3_exec(sql_fs->db, "PRAGMA cipher_page_size = 8192;", NULL, NULL, NULL);
    }
    else
        show_msg(stderr, "WARNING: No password set!\n");
#endif
    /* WAL mode improves the performance of write operations (page data must only be
     * written to disk one time) and improves concurrency by reducing blocking between
     * readers and writers */
    sqlite3_exec(sql_fs->db, "PRAGMA journal_mode = WAL;", NULL, NULL, NULL);

    /* Without this limit, the WAL file can grow without bounds.  When
     * extremely heavy loads, the WAL log can rapidly grow larger than
     * the database itself.  So set a limit here to prevent the disk
     * from filling with the WAL. */
    char buf[256];
    struct statvfs vfs;
    statvfs(db_file, &vfs);
    uint64_t limit = 10*1024*1024; // set min limit to 10MB
    // set dynamic limit to 10% of available space on partition
    uint64_t availableBytes = (uint64_t)vfs.f_bavail * vfs.f_bsize * 0.1;
    if (availableBytes > limit)
        limit = availableBytes;
    snprintf(buf, 256, "PRAGMA journal_size_limit = %"PRIu64";", limit);
    sqlite3_exec(sql_fs->db, buf, NULL, NULL, NULL);

    /* WAL mode only performs fsync on checkpoint operation, which reduces overhead
     * It should make it possible to run with synchronous set to NORMAL with less
     * of a performance impact.
    */
    sqlite3_exec(sql_fs->db, "PRAGMA synchronous = NORMAL;", NULL, NULL, NULL);

    /* It is vitally important that write operations not fail to execute due
     * to busy timeouts. Even using WAL, its still possible for a command to be
     * blocked due to attempted concurrent write operations. If this happens
     * without a busy handler, the write will fail and lead to corruption.
     *
     * Libsqlfs had attempted to do its own rudimentary busy handling via delay(),
     * however, its implementation seems to pre-date the availablity of busy
     * handlers in SQLite. Also, it is only used for some operations, and does not
     * protect many operations from failure.
     *
     * Thus, it is preferable to register SQLite's default busy handler with a
     * relatively high timeout to globally protect all operations. This is completely
     * transparent to the caller, and ensure that while a write operation might be
     * delayed for a period of time, it is unlikely that it will fail completely.
     *
     * An initial timeout for 10 seconds is set here, but could be increased to reduce
     * the chances of failure under high load.
     */
    sqlite3_busy_timeout(sql_fs->db, 10000);

    sql_fs->default_mode = 0700; /* allows the creation of children under / , default user at initialization is 0 (root)*/

    create_db_table(sql_fs);

    if (max_inode == 0)
        max_inode = get_current_max_inode(sql_fs);

    r = ensure_existence(sql_fs, "/", TYPE_DIR);
    if (!r)
        return 0;
    pthread_setspecific(pthread_key, sql_fs);
    instance_count++;
    return (void *) sql_fs;
}

static void sqlfs_t_finalize(void *arg)
{
    sqlfs_t *sql_fs = (sqlfs_t *) arg;
    if (sql_fs)
    {
        int i;
        for (i = 0; i < (int)(sizeof(sql_fs->stmts) / sizeof(sql_fs->stmts[0])); i++)
            if (sql_fs->stmts[i])
                sqlite3_finalize(sql_fs->stmts[i]);

        sqlite3_close(sql_fs->db);
        free(sql_fs);
        instance_count--;
    }
}


#ifdef HAVE_LIBSQLCIPHER

/* DANGER!  bytes must have exactly 32 bytes and buf must have at least 68!
 * buf is filled with 64 hex chars, the sqlcipher raw escape sequence (x''),
 * and the null terminator. This raw key data format is documented here:
 * http://sqlcipher.net/sqlcipher-api/#key */
static int generate_sqlcipher_raw_key(const uint8_t *bytes, size_t byteslen,
                                      char *buf, size_t buflen)
{
    int i;
    if (byteslen != REQUIRED_KEY_LENGTH)
    {
        show_msg(stderr, "Not %i bytes of raw key data! (%li bytes)\n",
                 REQUIRED_KEY_LENGTH, byteslen);
        return 0;
    }
    if (buflen < 68)
    {
        show_msg(stderr,
                 "Not enough room in buf to write the full key! (68 != %li)\n",
                 buflen);
        return 0;
    }
    memset(buf, 0, buflen);
    strncat(buf, "x'", buflen);
    for (i = 0; i < REQUIRED_KEY_LENGTH; i++)
    {
        buflen = buflen - 2;
        snprintf(buf + (i * 2) + 2, buflen, "%02X", bytes[i]);
    }
    snprintf(buf + (i * 2) + 2, buflen - 2, "'");
    if (strlen(buf) != 67)
    {
        show_msg(stderr,
                 "Raw key data string not 67 chars! (%li chars)\n",
                 strlen(buf));
        return 0;
    }
    return 1;
}

int sqlfs_open_key(const char *db_file, const uint8_t *key, size_t keylen, sqlfs_t **psqlfs)
{
    sqlfs_init_key(db_file, key, keylen);
    *psqlfs = sqlfs_t_init(db_file, cached_password);

    if (*psqlfs == 0)
        return 0;
    return 1;
}

int sqlfs_rekey(const char *db_file_name, const uint8_t *old_key, size_t old_key_len,
                const void *new_key, size_t new_key_len)
{
    char oldbuf[MAX_PASSWORD_LENGTH];
    char newbuf[MAX_PASSWORD_LENGTH];

    if (instance_count > 0)
    {
        show_msg(stderr, "ERROR: Cannot rekey on open sqlfs! (%i open)\n",
                 instance_count);
        return 0;
    }

    if (!generate_sqlcipher_raw_key(old_key, old_key_len, oldbuf, MAX_PASSWORD_LENGTH))
        return 0;
    if (!generate_sqlcipher_raw_key(new_key, new_key_len, newbuf, MAX_PASSWORD_LENGTH))
        return 0;
    int r = sqlfs_change_password(db_file_name, oldbuf, newbuf);
    memset(oldbuf, 0, MAX_PASSWORD_LENGTH); // zero out password
    memset(newbuf, 0, MAX_PASSWORD_LENGTH); // zero out password
    return r;
}

int sqlfs_open_password(const char *db_file, const char *password, sqlfs_t **psqlfs)
{
    sqlfs_init_password(db_file, password);
    *psqlfs = sqlfs_t_init(db_file, password);

    if (*psqlfs == 0)
        return 0;
    return 1;
}

int sqlfs_change_password(const char *db_file_name, const char *old_password, const char *new_password)
{
    int r;
    sqlfs_t *sqlfs;

    if (instance_count > 0)
    {
        show_msg(stderr, "ERROR: Cannot change password on open sqlfs! (%i open)\n",
                 instance_count);
        return 0;
    }

    if (!sqlfs_open_password(db_file_name, old_password, &sqlfs))
        return 0;
    r = sqlite3_rekey(sqlfs->db, new_password, strlen(new_password));
    if (r != SQLITE_OK)
    {
        show_msg(stderr, "ERROR: Failed to rekey database!\n");
        return 0;
    }
    return sqlfs_close(sqlfs);
}
#endif

int sqlfs_open(const char *db_file, sqlfs_t **psqlfs)
{
    sqlfs_init(db_file);
    *psqlfs = sqlfs_t_init(db_file, NULL);

    if (*psqlfs == 0)
        return 0;
    return 1;
}

int sqlfs_close(sqlfs_t *sqlfs)
{
    sqlfs_destroy();
    sqlfs_t_finalize(sqlfs);
    return !instance_count; // its an error if still instances left
}

void sqlfs_detach_thread(void)
{
    sqlfs_t_finalize(pthread_getspecific(pthread_key));
}


#ifdef HAVE_LIBFUSE


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
/* the xaddr functions are optional and can safely be left unimplemented
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
*/

static struct fuse_operations sqlfs_op;

#endif

int sqlfs_init(const char *db_file_name)
{
#ifdef HAVE_LIBFUSE
    sqlfs_op.getattr    = sqlfs_op_getattr;
    sqlfs_op.access     = sqlfs_op_access;
    sqlfs_op.readlink   = sqlfs_op_readlink;
    sqlfs_op.readdir    = sqlfs_op_readdir;
    sqlfs_op.mknod      = sqlfs_op_mknod;
    sqlfs_op.mkdir      = sqlfs_op_mkdir;
    sqlfs_op.symlink    = sqlfs_op_symlink;
    sqlfs_op.unlink     = sqlfs_op_unlink;
    sqlfs_op.rmdir      = sqlfs_op_rmdir;
    sqlfs_op.rename     = sqlfs_op_rename;
    sqlfs_op.link       = sqlfs_op_link;
    sqlfs_op.chmod      = sqlfs_op_chmod;
    sqlfs_op.chown      = sqlfs_op_chown;
    sqlfs_op.truncate   = sqlfs_op_truncate;
    sqlfs_op.utime      = sqlfs_op_utime;
    sqlfs_op.open       = sqlfs_op_open;
    sqlfs_op.create     = sqlfs_op_create;
    sqlfs_op.read       = sqlfs_op_read;
    sqlfs_op.write      = sqlfs_op_write;
    sqlfs_op.statfs     = sqlfs_op_statfs;
    sqlfs_op.release    = sqlfs_op_release;
    sqlfs_op.fsync      = sqlfs_op_fsync;
/* the xaddr functions are optional and can safely be left unimplemented
    sqlfs_op.setxattr   = sqlfs_op_setxattr;
    sqlfs_op.getxattr   = sqlfs_op_getxattr;
    sqlfs_op.listxattr  = sqlfs_op_listxattr;
    sqlfs_op.removexattr= sqlfs_op_removexattr;
*/
#endif

    if (db_file_name)
        strncpy(default_db_file, db_file_name, sizeof(default_db_file));
    pthread_key_create(&pthread_key, sqlfs_t_finalize);
    return 0;
}

int sqlfs_destroy()
{
    int err = pthread_key_delete(pthread_key);
    if (err == EINVAL)
        show_msg(stderr, "Invalid pthread key in sqlfs_destroy()!\n");
    /* zero out password in memory */
    memset(cached_password, 0, MAX_PASSWORD_LENGTH);
    return err;
}

#ifdef HAVE_LIBSQLCIPHER
int sqlfs_init_key(const char *db_file, const uint8_t *key, size_t keylen)
{
    char buf[MAX_PASSWORD_LENGTH];
    if (keylen != REQUIRED_KEY_LENGTH)
    {
        show_msg(stderr, "Raw key not exactly %i bytes! (%li bytes)\n",
                 REQUIRED_KEY_LENGTH, keylen);
        return 1;
    }
    if (!generate_sqlcipher_raw_key(key, keylen, buf, MAX_PASSWORD_LENGTH))
        return 1;
    strncpy(cached_password, buf, MAX_PASSWORD_LENGTH);
    memset(buf, 0, MAX_PASSWORD_LENGTH); // zero out password
    return sqlfs_init(db_file);
}

int sqlfs_init_password(const char *db_file, const char *password)
{
    if (strlen(password) > MAX_PASSWORD_LENGTH) {
        show_msg(stderr, "Password longer than MAX_PASSWORD_LENGTH (%li > %i)\n",
                 strlen(password), MAX_PASSWORD_LENGTH);
        return 1;
    }
    strncpy(cached_password, password, MAX_PASSWORD_LENGTH);
    return sqlfs_init(db_file);
}
#endif

int sqlfs_instance_count()
{
    return instance_count;
}

#ifdef HAVE_LIBFUSE

int sqlfs_fuse_main(int argc, char **argv)
{
    int ret = fuse_main(argc, argv, &sqlfs_op);
    /* zero out password in memory */
    memset(cached_password, 0, MAX_PASSWORD_LENGTH);
    return ret;
}

#endif

/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
