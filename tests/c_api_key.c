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
    int rc, i;
    sqlfs_t *sqlfs = 0;

    uint8_t firstkey[32] = {
        0x0a, 0xfc, 0x69, 0xa1, 0x16, 0x40, 0x4f, 0x7d, 0x7f, 0x1b, 0x1d, 0xb9,
        0x5e, 0x18, 0x11, 0x2e, 0x6b, 0x3c, 0xf7, 0x1e, 0x78, 0xaf, 0x88, 0x3c,
        0xb1, 0x90, 0x51, 0x15, 0xbf, 0xc3, 0xb2, 0x8d
    };
    uint8_t newkey[32] = {
        0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
        0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
        0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xaa, 0xbb
    };
    unsigned char charkey[32] = {
        0x0a, 0xfc, 0x69, 0xa1, 0x16, 0x40, 0x4f, 0x7d, 0x7f, 0x1b, 0x1d, 0xb9,
        0x5e, 0x18, 0x11, 0x2e, 0x6b, 0x3c, 0xf7, 0x1e, 0x78, 0xaf, 0x88, 0x3c,
        0xb1, 0x90, 0x51, 0x15, 0xbf, 0xc3, 0xb2, 0x8d
    };
    uint8_t testkey[32] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };

    if(argc > 1)
      database_filename = argv[1];
    if(exists(database_filename))
    {
       printf("\n(test database '%s' exists, deleting!)\n\n", database_filename);
       unlink(database_filename);
    }

    printf("Creating %s...", database_filename);
    rc = sqlfs_open_key(database_filename, firstkey, 32, &sqlfs);
    assert(rc);
    assert(sqlfs != 0);
    rc = sqlfs_close(sqlfs);
    assert(rc);
    printf("passed\n");

    printf("Opening database with too long a key...");
    sqlfs = 0;
    uint8_t longkey[33];
    memset(longkey, 5, 33);
    rc = sqlfs_open_key(database_filename, longkey, 33, &sqlfs);
    assert(!rc);
    assert(sqlfs == 0);
    printf("passed\n");

    printf("Opening database with too short a key...");
    sqlfs = 0;
    for (i = 0; i < 32; i++)
    {
        assert(!sqlfs_open_key(database_filename, firstkey, i, &sqlfs));
        assert(sqlfs == 0);
    }
    printf("passed\n");

    printf("Opening database with wrong key...");
    sqlfs = 0;
    rc = sqlfs_open_key(database_filename, testkey, 32, &sqlfs);
    assert(!rc);
    assert(sqlfs == 0);
    printf("passed\n");

    printf("Opening database with correct key...");
    sqlfs = 0;
    rc = sqlfs_open_key(database_filename, firstkey, 32, &sqlfs);
    assert(rc);
    assert(sqlfs != 0);
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    printf("Opening database with correct key as char...");
    sqlfs = 0;
    rc = sqlfs_open_key(database_filename, charkey, 32, &sqlfs);
    assert(rc);
    assert(sqlfs != 0);
    printf("passed\n");

    run_standard_tests(sqlfs);

    printf("Closing database...");
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    assert(sqlfs_instance_count() == 0);

    printf("Testing direct SQL command...");
    assert(sqlfs_open_key(database_filename, firstkey, 32, &sqlfs));
    assert(sqlite3_exec(sqlfs->db, "SELECT count(*) FROM sqlite_master;", NULL, NULL, NULL) == SQLITE_OK);
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    printf("Attempting to change key for mounted VFS...");
    sqlfs = 0;
    assert(sqlfs_open_key(database_filename, firstkey, 32, &sqlfs));
    assert(sqlfs != 0);
    assert(!sqlfs_rekey(database_filename, firstkey, 32, newkey, 32));
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    assert(sqlfs_instance_count() == 0);

    printf("Change password for unmounted VFS...");
    assert(sqlfs_rekey(database_filename, firstkey, 32, newkey, 32));
    printf("passed\n");

    printf("Mounting database with new key...");
    rc = sqlfs_open_key(database_filename, firstkey, 32, &sqlfs);
    assert(!rc);
    rc = sqlfs_open_key(database_filename, newkey, 32, &sqlfs);
    assert(rc);
    assert(sqlfs_close(sqlfs));
    printf("passed\n");

    assert(sqlfs_instance_count() == 0);

    printf("Changing key of unmounted VFS again...");
    assert(sqlfs_rekey(database_filename, newkey, 32, testkey, 32));
    printf("passed\n");

    rc++; // silence ccpcheck

    return 0;
}


/* -*- mode: c; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4; c-file-style: "bsd"; -*- */
