INCLUDES = -I$(top_srcdir)/src/

TESTS= \
	create_table.test \
	delete_item.test \
	batch_get_item.test \
	batch_write_item.test \
	describe_table.test \
	get_item.test \
	query.test \
	scan.test \
	update_item.test \
	sigv4.test

noinst_PROGRAMS = $(TESTS)

LDADD = $(top_builddir)/test/libaws_dynamo_test_utils.la \
	$(top_builddir)/src/libaws_dynamo.la

noinst_LTLIBRARIES = libaws_dynamo_test_utils.la
libaws_dynamo_test_utils_la_SOURCES=test_utils.c
