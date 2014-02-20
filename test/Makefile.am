INCLUDES = -I$(top_srcdir)/src/

TESTS= \
	aws_dynamo_create_table_tests \
	aws_dynamo_delete_item_tests \
	aws_dynamo_batch_get_item_tests \
	aws_dynamo_describe_table_tests \
	aws_dynamo_get_item_tests \
	aws_dynamo_query_tests \
	aws_dynamo_scan_tests \
	aws_dynamo_update_item_tests

noinst_PROGRAMS = $(TESTS)

LDADD = $(top_builddir)/test/libaws_dynamo_test_utils.la \
	$(top_builddir)/src/libaws_dynamo.la

noinst_LTLIBRARIES = libaws_dynamo_test_utils.la
libaws_dynamo_test_utils_la_SOURCES=test_utils.c
