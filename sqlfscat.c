#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "sqlfs.h"

#define BUF_SIZE 8192

int main(int argc, char *argv[])
{
    int n;
    char buf[BUF_SIZE];
    struct stat s;
    sqlfs_t *sqlfs = 0;
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s sqlfs.db /path/to/file/to/cat\n", argv[0]);
        exit(1);
    }
    const char *db = argv[1];
    const char *file = argv[2];

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
    if (S_ISREG(s.st_mode))
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

/* now read the file from sqlfs */
    struct fuse_file_info ffi;
    int wrote = 0; // this is used to track were we are in the read
    sqlfs_proc_open(sqlfs, file, &ffi);
    while((n = sqlfs_proc_read(sqlfs, file, buf, BUF_SIZE, wrote, &ffi)) > 0)
    {
        wrote += fwrite(buf, sizeof(char), n, stdout);
    }

    sqlfs_close(sqlfs);
    // TODO return proper exit value
    return 0;
}
