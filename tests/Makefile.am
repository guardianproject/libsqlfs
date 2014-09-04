AM_CFLAGS = -I$(top_srcdir) @DEFS@
AM_LDFLAGS = $(top_builddir)/libsqlfs-1.0.la -lpthread @SQLITE@ @LIBFUSE@

EXTRA_DIST = defs

TESTS_ENVIRONMENT = top_builddir=$(top_builddir)
testsubdir = .tests
bashtestsubdir = fuse_test

# scripted tests
check_SCRIPTS = \
	c_perf.test \
	c_api.test \
	c_thread_api.test

# these scripts require root/fuse access to mount the filesystem before testing
#	fuse_sqlfs.test \
#	directory_rename.test

# compiled tests
check_PROGRAMS = \
	c_api \
	c_thread_api \
	c_perf \
	fsx

c_api_SOURCES = c_api.c
c_thread_api_SOURCES = c_thread_api.c
c_perf_SOURCES = c_perf.c
fsx_SOURCES = fsx.c
fsx_CFLAGS = -Wno-error=unused-result

if WITH_SQLCIPHER
check_SCRIPTS += c_perf_key.test c_api_key.test c_thread_api_key.test
check_SCRIPTS += c_api_password.test
check_PROGRAMS += c_perf_key c_api_key c_thread_api_key
check_PROGRAMS += c_api_password
c_perf_key_SOURCES = c_perf_key.c
c_api_key_SOURCES = c_api_key.c
c_thread_api_key_SOURCES = c_thread_api_key.c
c_api_password_SOURCES = c_api_password.c
endif

TESTS = $(check_SCRIPTS)

distclean-local:
	-rm -rf $(testsubdir)
	-rm -rf $(bashtestsubdir)

