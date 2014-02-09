INCLUDES = -I$(top_srcdir)/src/

TESTS=aws_dynamo_create_table_tests

noinst_PROGRAMS = $(TESTS)
aws_dynamo_create_table_tests_SOURCES=aws_dynamo_create_table_tests.c

LDADD = $(top_builddir)/src/libaws_dynamo.la

#TESTS= \
#	aws_dynamo_tests.c \
#	aws_dynamo_batch_get_item_tests.c \
#	aws_dynamo_get_item_tests.c \
#	aws_dynamo_query_tests.c \
#	aws_dynamo_scan_tests.c \
#	aws_dynamo_update_item_tests.c \
#	aws_iam_tests.c
