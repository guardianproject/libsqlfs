AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4
AM_CFLAGS = -I$(top_srcdir) @DEFS@

SUBDIRS = . tests

include_HEADERS = sqlfs.h sqlfs_internal.h
lib_LTLIBRARIES = libsqlfs-1.0.la

libsqlfs_1_0_la_SOURCES = sqlfs.c
libsqlfs_1_0_la_LIBADD = @SQLITE@ @LIBFUSE@
libsqlfs_1_0_la_LDFLAGS = -version-info 1:0:0

bin_PROGRAMS = sqlfscat sqlfsls
sqlfscat_SOURCES = sqlfscat.c
sqlfscat_LDADD = -lpthread @SQLITE@ ./libsqlfs-1.0.la
sqlfsls_SOURCES = sqlfsls.cpp
sqlfsls_LDADD = -lpthread @SQLITE@ ./libsqlfs-1.0.la

if WITH_LIBFUSE
bin_PROGRAMS += fuse_sqlfs
fuse_sqlfs_SOURCES = fuse_sqlfs.c
fuse_sqlfs_LDADD = -lpthread @SQLITE@ @LIBFUSE@ ./libsqlfs-1.0.la
endif

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libsqlfs.pc
EXTRA_DIST = libsqlfs.pc.in
