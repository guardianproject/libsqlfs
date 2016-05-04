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

#include "sqlfs.h"
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv)
{
    int rc;
    const char* db = "/tmp/fsdata";
#ifdef HAVE_LIBSQLCIPHER
    sqlfs_t *sqlfs = 0;

/* get the password from stdin */
#  define BUF_SIZE 8192
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
    else // this is for the sqlfs_init() line below
#endif /* HAVE_LIBSQLCIPHER */
/* if you want to mount a file with a password */
        sqlfs_init(db);

    rc = sqlfs_fuse_main(argc, argv);
    sqlfs_destroy();
    return rc;
}


/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
