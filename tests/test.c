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
#include "sqlfs.h"

int main(int argc, char *argv[])
{
    char *data = "this is a string";
    char *data2 = malloc(sizeof(char) * 32000);
    char buf[200];
    struct fuse_file_info fi = { 0 };
    int i;
    sqlfs_t *sqlfs = 0;
    memset(data2, '2', 32000);
    sqlfs_init(argv[1]);
    sqlfs_open(argv[1], &sqlfs);
    
    sqlfs_proc_mkdir(sqlfs, "/test", 0666);
    sqlfs_proc_mkdir(sqlfs, "/test/1", 0666);
    sqlfs_proc_mkdir(sqlfs, "/test/2", 0666);
    sqlfs_proc_mkdir(sqlfs, "/a/b/c/d/e/f/g", 0666);
    fi.flags |= ~0;
    sqlfs_proc_write(sqlfs, "/test/2/file", data, strlen(data), 0, &fi);
    sqlfs_proc_rmdir(sqlfs, "/test");
    
    
    i = sqlfs_proc_read(sqlfs, "/test/2/file", buf, sizeof(buf), 0, &fi);
    buf[i] = 0;
    assert(!strcmp(buf, data));
    
    sqlfs_proc_write(sqlfs, "/app/data/file", data, strlen(data), 0, &fi);
    sqlfs_proc_write(sqlfs, "/big/a/b/c/file", data2, 32000, 0, &fi);

    i = sqlfs_proc_read(sqlfs, "/big/a/b/c/file", buf, sizeof(buf), 400, &fi);
    assert(buf[97] == '2');
    sqlfs_close(sqlfs);

}

