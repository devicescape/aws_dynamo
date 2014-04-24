AM_CPPFLAGS = -I$(top_srcdir)/src/

TESTS= \
	batch_get_item.test \
	batch_write_item.test \
	create_table.test \
	delete_item.test \
	describe_table.test \
	get_item.test \
	list_tables.test \
	put_item.test \
	query.test \
	setup.test
	scan.test \
	sigv4.test \
	update_item.test

# Test dependancies are specified by specifying dependancies between the
# log files.
# setup.test needs to be run before any other test is run.
create_table.log: setup.log
delete_item.log: setup.log
batch_get_item.log: setup.log
batch_write_item.log: setup.log
describe_table.log: setup.log
get_item.log: setup.log
list_tables.log: setup.log
put_item.log: setup.log
query.log: setup.log
scan.log: setup.log
update_item.log: setup.log
sigv4.log: setup.log

noinst_PROGRAMS = $(TESTS)

LDADD = $(top_builddir)/test/libaws_dynamo_test_utils.la \
	$(top_builddir)/src/libaws_dynamo.la

noinst_LTLIBRARIES = libaws_dynamo_test_utils.la
libaws_dynamo_test_utils_la_SOURCES=test_utils.c
