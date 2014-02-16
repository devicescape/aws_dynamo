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

LDADD = $(top_builddir)/src/libaws_dynamo.la

