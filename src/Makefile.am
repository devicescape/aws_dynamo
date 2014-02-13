lib_LTLIBRARIES = libaws_dynamo.la
libaws_dynamo_la_SOURCES = aws.c aws_dynamo.c \
	aws_dynamo_batch_get_item.c aws_dynamo_cmd.c \
	aws_dynamo_create_table.c aws_dynamo_get_item.c aws_dynamo_put_item.c \
	aws_dynamo_query.c aws_dynamo_scan.c \
	aws_dynamo_update_item.c aws_iam.c aws_sts.c http.c \
	aws_dynamo_delete_table.c \
	aws_dynamo_describe_table.c \
	aws_dynamo_update_table.c \
	aws_dynamo_utils.h \
	aws_iam.h \
	aws_sts.h \
	http.h

libaws_dynamo_la_CPPFLAGS = -I$(top_srcdir)/src

pkginclude_HEADERS=\
	aws_dynamo_batch_get_item.h \
	aws_dynamo_create_table.h \
	aws_dynamo_delete_table.h \
	aws_dynamo_describe_table.h \
	aws_dynamo_get_item.h \
	aws_dynamo.h \
	aws_dynamo_put_item.h \
	aws_dynamo_query.h \
	aws_dynamo_scan.h \
	aws_dynamo_update_item.h \
	aws_dynamo_update_table.h \
	aws.h
