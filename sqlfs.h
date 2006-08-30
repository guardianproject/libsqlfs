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
/*!
 * @file sqlfs.h
 *
 * @brief File public header for file system on top of a SQL database library
 *  APIs
 *
 *****************************************************************************/


#ifndef __SQLFS__H__
#define __SQLFS__H__

#ifdef __cplusplus
extern "C" {
#endif


#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>



#ifndef FUSE

/* the following struct derived from the FUSE header file 

    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2006  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU LGPL.
*/

/**
 * Information about open files
 *
 * Changed in version 2.5
 */
struct fuse_file_info {
    /** Open flags.  Available in open() and release() */
    int flags;

    /** Old file handle, don't use */
    unsigned long fh_old;

    /** In case of a write operation indicates if this was caused by a
        writepage */
    int writepage;

    /** Can be filled in by open, to use direct I/O on this file.
        Introduced in version 2.4 */
    unsigned int direct_io : 1;

    /** Can be filled in by open, to indicate, that cached file data
        need not be invalidated.  Introduced in version 2.4 */
    unsigned int keep_cache : 1;

    /** Padding.  Do not use*/
    unsigned int padding : 30;

    /** File handle.  May be filled in by filesystem in open().
        Available in all other file operations */
    uint64_t fh;
};


/** Function to add an entry in a readdir() operation
 *
 * @param buf the buffer passed to the readdir() operation
 * @param name the file name of the directory entry
 * @param stat file attributes, can be NULL
 * @param off offset of the next entry or zero
 * @return 1 if buffer is full, zero otherwise
 */
typedef int (*fuse_fill_dir_t) (void *buf, const char *name,
                                const struct stat *stbuf, off_t off);


#else

#include "fuse.h"

#endif

#include "sqlfs_internal.h"

int sqlfs_init(const char *);

#ifdef FUSE
int sqlfs_fuse_main(int argc, char **argv);
#endif

     

#ifdef __cplusplus
}       // extern "C"
#endif

#endif

                  
                  


