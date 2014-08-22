#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>
#include <iostream>

#include "sqlfs.h"

#define BUF_SIZE 8192

typedef std::vector<std::string> DirEntries;

/* FUSE filler() function for use with the FUSE style readdir() that
 * libsqlfs provides.  Note: this function only ever expects statp to
 * be NULL and off to be 0.  buf is DirEntries */
static int fill_dir(void *buf, const char *name, const struct stat *statp, off_t off) {
    DirEntries *entries = (DirEntries*) buf;
    if(statp != NULL)
        fprintf(stderr, "File.listImpl() fill_dir always expects statp to be NULL");
    if(off != 0)
        fprintf(stderr, "File.listImpl() fill_dir always expects off to be 0");
    entries->push_back(name);
    // TODO implement returning an error (1) if something bad happened
    return 0;
}

int main(int argc, char *argv[])
{
    struct stat s;
    sqlfs_t *sqlfs = 0;
    const char *db;
    const char *file;
    if (argc == 2)
    {
        db = argv[1];
        file = "/";        
    }
    else if (argc != 3)
    {
        fprintf(stderr, "Usage: %s sqlfs.db [ /path ]\n", argv[0]);
        exit(1);
    }
    else
    {
        db = argv[1];
        file = argv[2];
    }

/* first setup the database file */
    if(access(db, R_OK))
    {
        fprintf(stderr, "sqlfs file is not readable! (%s)\n", db);
        exit(1);
    }
    if(stat(db, &s) < 0)
    {
        fprintf(stderr, "stat on %s failed!\n", db);
        exit(1);
    }
    if(S_ISREG(s.st_mode)<0)
    {
        fprintf(stderr, "Not a regular file: %s\n", db);
        exit(1);
    }


#ifdef HAVE_LIBSQLCIPHER
/* get the password from stdin */
    char password[BUF_SIZE];
    char *p = fgets(password, BUF_SIZE, stdin);
    if (p)
    {
        /* remove trailing newline */
        size_t last = strlen(p) - 1;
        if (p[last] == '\n')
            p[last] = '\0';
        if (!sqlfs_open_password(db, password, &sqlfs)) {
            fprintf(stderr, "Failed to open: %s\n", db);
            return 1;
        }
        sqlfs_init_password(db, password);
        memset(password, 0, BUF_SIZE); // zero out password
    }
    else
#endif /* HAVE_LIBSQLCIPHER */
    {
        if (!sqlfs_open(db, &sqlfs)) {
            fprintf(stderr, "Failed to open: %s\n", db);
            return 1;
        }
        sqlfs_init(db);
    }

    if (!sqlfs_proc_access(sqlfs, file, R_OK)) {
        fprintf(stderr, "Cannot access %s in %s\n", file, db);
        //return 1;
    }

/* now read the dir entries */
    DirEntries entries;
    // using FUSE readdir in old getdir() style which gives us the whole thing at once
    int ret = sqlfs_proc_readdir(0, file, (void *)&entries, (fuse_fill_dir_t)fill_dir, 0, NULL);
    for(DirEntries::const_iterator i = entries.begin() + 2; i != entries.end(); ++i) {
        std::cout << *i << "\n"; // this will print all the contents of *features*
    }

    sqlfs_close(sqlfs);

    return abs(ret); // FUSE returns negative error values
}
