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
    char *database_filename = "c_api_password.db";
    char *first_password = "First Password";
    char *new_password = "New Password";
    int rc, i;
    sqlfs_t *sqlfs = 0;

    if(argc > 1)
      database_filename = argv[1];
    if(argc > 2)
      first_password = argv[2];
    if(exists(database_filename))
    {
       printf("\n(test database '%s' exists, deleting!)\n\n", database_filename);
       unlink(database_filename);
    }

    printf("Creating %s...", database_filename);
    rc = sqlfs_open_password(database_filename, first_password, &sqlfs);
    assert(rc);
    assert(sqlfs != 0);
    rc = sqlfs_close(sqlfs);
    assert(rc);
    printf("passed\n");

    printf("Opening database with too long password...");
    sqlfs = 0;
    char long_pass[600];
    for(i = 0; i < 600; ++i)
        long_pass[i] = 'A';
    long_pass[599] = '\0';
    rc = sqlfs_open_password(database_filename, long_pass, &sqlfs);
    assert(!rc);
    assert(sqlfs == 0);
    printf("passed\n");

    printf("Opening database with wrong password...");
    sqlfs = 0;
    rc = sqlfs_open_password(database_filename, "fakesecret", &sqlfs);
    assert(!rc);
    assert(sqlfs == 0);
    printf("passed\n");

    printf("Opening database with correct password...");
    sqlfs = 0;
    rc = sqlfs_open_password(database_filename, first_password, &sqlfs);
    assert(rc);
    assert(sqlfs != 0);
    printf("passed\n");

    run_standard_tests(sqlfs);

    printf("Closing database...");
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    assert(sqlfs_instance_count() == 0);

    printf("Testing direct SQL command...");
    assert(sqlfs_open_password(database_filename, first_password, &sqlfs));
    assert(sqlite3_exec(sqlfs->db, "SELECT count(*) FROM sqlite_master;", NULL, NULL, NULL) == SQLITE_OK);
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    printf("Attempting to change password for mounted VFS...");
    assert(sqlfs_open_password(database_filename, first_password, &sqlfs) );
    assert(!sqlfs_change_password(database_filename, first_password, new_password) );
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    assert(sqlfs_instance_count() == 0);

    printf("Change password for unmounted VFS...");
    assert(sqlfs_change_password(database_filename, first_password, new_password) );
    printf("passed\n");

    printf("Mounting database with new password...");
    rc = sqlfs_open_password(database_filename, first_password, &sqlfs);
    assert(!rc);
    rc = sqlfs_open_password(database_filename, new_password, &sqlfs);
    assert(rc);
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    assert(sqlfs_instance_count() == 0);

    printf("Changing password of unmounted VFS again...");
    assert(sqlfs_change_password(database_filename, new_password, "some random garbage"));
    printf("passed\n");

    rc++; // silence ccpcheck

    return 0;
}


/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
