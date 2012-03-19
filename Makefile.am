AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

include $(top_srcdir)/makerules

SUBDIRS = . tests

include_HEADERS = sqlfs.h sqlfs_internal.h
lib_LTLIBRARIES = libsqlfs-1.0.la

libsqlfs_1_0_la_SOURCES = sqlfs.c
libsqlfs_1_0_la_LIBADD = $(DEPS_LIBS)
libsqlfs_1_0_la_LDFLAGS = -version-info 1:0:0

if FUSE
bin_PROGRAMS = fuse_sqlfs
fuse_sqlfs_SOURCES = fuse_main.c
fuse_sqlfs_CFLAGS = -D_FILE_OFFSET_BITS=64 -D_REENTRANT -DFUSE_USE_VERSION=25
fuse_sqlfs_LDADD = -lpthread $(DEPS_LIBS)
endif

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libsqlfs.pc
EXTRA_DIST = libsqlfs.pc.in



