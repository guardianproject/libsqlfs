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

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "sqlfs.h"

int exists(char *filename)
{
    FILE *file;
    if ((file = fopen(filename, "r")) == NULL) {
        if (errno == ENOENT) {
            return 0;
        } else {
            // Check for other errors too, like EACCES and EISDIR
            printf("Error occured: %d", errno);
        }
        return 0;
    } else {
        fclose(file);
    }
    return 1;
}

int main(int argc, char *argv[])
{
    char *database_filename = "test.db";
    char *data = "this is a string";
    char *testfilename = "";
    char buf[200];
    struct stat sb;
    struct fuse_file_info ffi;

    struct fuse_file_info fi = { 0 };
    int i, rc;
    sqlfs_t *sqlfs = 0;

    if(argc > 1)
      database_filename = argv[1];
    if(exists(database_filename))
       printf("%s exists.\n", database_filename);
    printf("Opening %s\n", database_filename);
    sqlfs_init(database_filename);
    printf("Running tests:\n");

    rc = sqlfs_open(database_filename, &sqlfs);
    printf("Opening database...");
    assert(rc);
    printf("passed\n");

    printf("Testing mkdir with sleep...");
    testfilename = "/mkdir-with-sleep0";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    sleep(1);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    testfilename = "/mkdir-with-sleep1";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    sleep(1);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");

    printf("Testing mkdir without sleep...");
    testfilename = "/mkdir-without-sleep0";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");

    printf("Testing whether mkdir does not make nested dirs...");
    testfilename = "/a/b/c/d/e/f/g";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    assert(!sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");

    printf("Testing mkdir to make nested dirs one at a time...");
    sqlfs_proc_mkdir(sqlfs, "/test", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test"));
    sqlfs_proc_mkdir(sqlfs, "/test/1", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test/1"));
    sqlfs_proc_mkdir(sqlfs, "/test/1/2", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test/1/2"));
    printf("passed\n");

    printf("Testing rmdir...");
    testfilename = "/mkdir-to-rmdir";
    sqlfs_proc_mkdir(sqlfs, testfilename, 0777);
    assert(sqlfs_is_dir(sqlfs, testfilename));
    sqlfs_proc_rmdir(sqlfs, testfilename);
    assert(!sqlfs_is_dir(sqlfs, testfilename));
    printf("passed\n");

    printf("Testing creating a file with a small string...", buf, data);
    fi.flags |= ~0;
    sqlfs_proc_mkdir(sqlfs, "/bufdir", 0777);
    sqlfs_proc_write(sqlfs, "/bufdir/file", data, strlen(data), 0, &fi);
    assert(!sqlfs_is_dir(sqlfs, "/bufdir/file"));
    i = sqlfs_proc_read(sqlfs, "/bufdir/file", buf, sizeof(buf), 0, &fi);
    buf[i] = 0;
    assert(!strcmp(buf, data));
    printf("passed\n");

#define TESTSIZE 1000000
    printf("Testing writing %d bytes of data...", TESTSIZE);
    testfilename = "/randomdata";
    char randombuf[TESTSIZE];
    char randomdata[TESTSIZE];
    for (i=0; i<TESTSIZE; ++i)
        randomdata[i] = (i % 90) + 32;
    randomdata[TESTSIZE-1] = 0;
    sqlfs_proc_write(sqlfs, testfilename, randomdata, TESTSIZE, 0, &fi);
    sleep(1);
    i = sqlfs_proc_read(sqlfs, testfilename, randombuf, TESTSIZE, 0, &fi);
    randombuf[i] = 0;
    assert(!strcmp(randombuf, randomdata));
    printf("passed\n");

    printf("Testing reading while requesting more bytes than will fit in the buffer...");
    i = sqlfs_proc_read(sqlfs, testfilename, buf, sizeof(buf), sizeof(buf)*2, &fi);
    assert(i==sizeof(buf));
    printf("passed\n");

    printf("Testing reading a byte with offset 10000 times...");
    int y, readloc;
    for(y=0; y<10000; y++)
    {
        readloc = (float)rand() / RAND_MAX * (TESTSIZE-1);
        i = sqlfs_proc_read(sqlfs, testfilename, buf, 1, readloc, &fi);
        assert(i == 1);
        assert(buf[0] == (readloc % 90) + 32);
    }
    printf("passed\n");

    printf("Testing truncating...");
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == TESTSIZE);
    sqlfs_proc_truncate(sqlfs, testfilename, 0);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");

    printf("Testing opening existing file truncation...");
    sqlfs_proc_write(sqlfs, testfilename, randomdata, TESTSIZE, 0, &fi);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == TESTSIZE);
    ffi.flags = O_WRONLY | O_CREAT | O_TRUNC;
    ffi.direct_io = 0;
    rc = sqlfs_proc_open(sqlfs, testfilename, &ffi);
    assert(rc == 0);
    sqlfs_proc_getattr(sqlfs, testfilename, &sb);
    assert(sb.st_size == 0);
    printf("passed\n");

    printf("Closing database...");
    sqlfs_close(sqlfs);
    printf("done\n");
    return 0;
}

