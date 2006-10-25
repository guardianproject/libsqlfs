AUTOMAKE_OPTIONS = foreign

include $(top_srcdir)/makerules

SUBDIRS = . tests

include_HEADERS = sqlfs.h sqlfs_internal.h
lib_LTLIBRARIES = libsqlfs.la

libsqlfs_la_SOURCES = sqlfs.c
libsqlfs_la_LIBADD = $(DEPS_LIBS)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libsqlfs.pc
EXTRA_DIST = libsqlfs.pc.in



