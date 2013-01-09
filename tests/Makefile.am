AM_CFLAGS = -I$(top_srcdir) @DEFS@
AM_LDFLAGS = $(top_builddir)/libsqlfs-1.0.la -lpthread @SQLITE@ @LIBFUSE@

EXTRA_DIST = defs

TESTS_ENVIRONMENT = top_builddir=$(top_builddir)
testsubdir = .tests
bashtestsubdir = fuse_test

# scripted tests
check_SCRIPTS = \
	c_api.test \
	c_thread__api.test \
	fuse_sqlfs.test \
	directory_rename.test

# compiled tests
check_PROGRAMS = \
	c_api \
	c_thread_api \
	fsx

c_api_SOURCES = c_api.c
c_thread_api_SOURCES = c_thread_api.c
fsx_SOURCES = fsx.c

if WITH_SQLCIPHER
check_SCRIPTS += c_api_key.test
check_PROGRAMS += c_api_key
c_api_key_SOURCES = c_api_key.c
endif

TESTS = $(check_SCRIPTS)

distclean-local:
	-rm -rf $(testsubdir)
	-rm -rf $(bashtestsubdir)

