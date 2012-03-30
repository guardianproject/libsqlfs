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
            printf("File doesn't exist");
        } else {
            // Check for other errors too, like EACCES and EISDIR
            printf("Some other error occured");
        }
        return 0;
    } else {
        fclose(file);
    }
    return 1;
}

int main(int argc, char *argv[])
{
    char *filename = "test.db";
    char *data = "this is a string";
    char *data2 = malloc(sizeof(char) * 32000);
    char buf[200];

    struct fuse_file_info fi = { 0 };
    int i, rc;
    sqlfs_t *sqlfs = 0;
    memset(data2, '2', 32000);

    if(argc > 1)
      filename = argv[1];
    if(exists(filename))
       printf("%s exists.\n", filename);
    printf("Opening %s\n", filename);
    sqlfs_init(filename);
    printf("Running tests:\n");

    rc = sqlfs_open(filename, &sqlfs);
    printf("Opening database...");
    assert(rc);
    printf("passed\n");

    sqlfs_proc_mkdir(sqlfs, "/test", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test"));
    sqlfs_proc_mkdir(sqlfs, "/test/1", 0777);
    assert(sqlfs_is_dir(sqlfs, "/test/1"));
    sqlfs_proc_mkdir(sqlfs, "/test/2", 0777);
    sleep(1);
    assert(sqlfs_is_dir(sqlfs, "/test/2"));
    sqlfs_proc_mkdir(sqlfs, "/a/b/c/d/e/f/g", 0777);

    fi.flags |= ~0;
    sqlfs_proc_write(sqlfs, "/test/2/file", data, strlen(data), 0, &fi);
    assert(!sqlfs_is_dir(sqlfs, "/test/2/file"));
    sqlfs_proc_rmdir(sqlfs, "/test");
    assert(sqlfs_is_dir(sqlfs, "/test"));
    i = sqlfs_proc_read(sqlfs, "/test/2/file", buf, sizeof(buf), 0, &fi);
    buf[i] = 0;
    printf("buf(%s) == data(%s)...", buf, data);
    assert(!strcmp(buf, data));
    printf("passed\n");

    char randombuf[100000];
    char randomdata[100000];
    for (i=0; i<100000; ++i)
	randomdata[i] = (i % 90) + 32;
    randomdata[99999] = 0;
    sqlfs_proc_write(sqlfs, "/randomdata", randomdata, strlen(randomdata), 0, &fi);
    sleep(1);
    i = sqlfs_proc_read(sqlfs, "/randomdata", randombuf, sizeof(randombuf), 0, &fi);
    randombuf[i] = 0;
    printf("randombuf == randomdata...");
    assert(!strcmp(randombuf, randomdata));
    printf("passed\n");

    sqlfs_proc_write(sqlfs, "/app/data/file", data, strlen(data), 0, &fi);
    sqlfs_proc_write(sqlfs, "/big/a/b/c/file", data2, 32000, 0, &fi);

    i = sqlfs_proc_read(sqlfs, "/big/a/b/c/file", buf, sizeof(buf), 400, &fi);
    assert(buf[97] == '2');
    sleep(1);
    printf("Closing database...\n");
    sqlfs_close(sqlfs);
    return 0;
}

