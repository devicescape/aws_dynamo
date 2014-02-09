lib_LTLIBRARIES = libaws_dynamo.la
libaws_dynamo_la_SOURCES = aws.c aws_dynamo.c \
	aws_dynamo_batch_get_item.c aws_dynamo_cmd.c \
	aws_dynamo_create_table.c aws_dynamo_get_item.c aws_dynamo_put_item.c \
	aws_dynamo_query.c aws_dynamo_scan.c \
	aws_dynamo_update_item.c aws_iam.c aws_sts.c http.c
libaws_dynamo_la_CPPFLAGS = -I$(top_srcdir)/src
	
TESTS=aws_dynamo_create_table_tests

#TESTS= \
#	aws_dynamo_tests.c \
#	aws_dynamo_batch_get_item_tests.c \
#	aws_dynamo_get_item_tests.c \
#	aws_dynamo_query_tests.c \
#	aws_dynamo_scan_tests.c \
#	aws_dynamo_update_item_tests.c \
#	aws_iam_tests.c
