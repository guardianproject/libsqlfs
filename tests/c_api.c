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

#include "common.c"

int main(int argc, char *argv[])
{
    char *database_filename = "c_api.db";
    int rc;
    sqlfs_t *sqlfs = 0;

    if(argc > 1)
      database_filename = argv[1];
    if(exists(database_filename))
    {
       printf("\n(test database '%s' exists, deleting!)\n\n", database_filename);
       unlink(database_filename);
    }

    printf("Opening %s...", database_filename);
    rc = sqlfs_open(database_filename, &sqlfs);
    assert(rc);
    assert(sqlfs != 0);
    printf("passed\n");

    run_standard_tests(sqlfs);

    printf("Closing database...");
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    rc++; // silence ccpcheck

    return 0;
}


/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
