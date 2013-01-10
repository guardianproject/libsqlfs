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
    char *database_filename = "c_api_key.db";
    char *database_password = "mytestpassword";
    int rc;
    sqlfs_t *sqlfs = 0;

    if(argc > 1)
      database_filename = argv[1];
    if(argc > 2)
      database_password = argv[2];
    if(exists(database_filename))
    {
       printf("\n(test database '%s' exists, deleting!)\n\n", database_filename);
       unlink(database_filename);
    }
    printf("Creating %s...", database_filename);
    sqlfs_init(database_filename);
    rc = sqlfs_open_key(database_filename, database_password, &sqlfs);
    assert(rc);
    rc = sqlfs_close(sqlfs);
    assert(rc);
    printf("passed\n");

    printf("Opening database with wrong password...");
    rc = sqlfs_open_key(argv[1], "fakesecret", &sqlfs);
    assert(!rc);
    printf("passed\n");

    printf("Opening database with correct password...");
    rc = sqlfs_open_key(database_filename, database_password, &sqlfs);
    assert(rc);
    printf("passed\n");

    run_standard_tests(sqlfs);

    printf("Closing database...");
    sqlfs_close(sqlfs);
    printf("done\n");
    return 0;
}

