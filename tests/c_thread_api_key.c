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

/* this version deliberately passes NULL for sqlfs to test the
   pthread_key_t in the style of the sqlfs_op_* functions */

#include "common.c"

int main(int argc, char *argv[])
{
    int rc;
    char *database_filename = "c_thread_api_key.db";

    if(argc > 1)
      database_filename = argv[1];
    if(exists(database_filename))
       printf("%s exists.\n", database_filename);

    printf("Opening %s\n", database_filename);
    rc = sqlfs_init_password(database_filename, "mysupersecretpassword");
    assert(rc == 0);

    run_standard_tests(NULL);

    printf("Destroying:\n");
    rc = sqlfs_destroy();
    assert(rc == 0);

    rc++; // silence ccpcheck

    printf("done\n");
    return 0;
}


/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
