aws_dynamo
==========

AWS DynamoDB Library for C

Features
========

* AWS DynamoDB API version 2011-12-05 (DynamoDB v1 protocol)
* Supports the following DynamoDB operations: batch get item,
  create table, get item, put item, query, scan, update item
* A flexible, efficient, and fast API for accessing DynamoDB
  from within C applications.
* Supports obtaining AWS credentials from an IAM Role,
  environment variables or initialization parameters

Supported Systems
=================

This library has been developed and tested on GNU/Linux.  That said,
this library attempts to be portable to wherever the dependacies
listed below are available.  Patches to increase portability or
reports of portability successes or failures are appreciated.

Dependancies
============

* libcurl - for http support, http://curl.haxx.se/libcurl/
* libyajl - for json parsing, http://lloyd.github.io/yajl/
* expat - for xml parsing, http://expat.sourceforge.net/
* openssl - for general purpose crypto functions, https://www.openssl.org/

Building
========

...
$ ./autogen.sh
$ ./configure
$ make
...

Then, to run tests:
...
$ make check
...

Basic Usage
===========

Get item attributes from DynamoDB.  Assume we have a table named
'users' with a string hash key and attributes 'realName'
and 'lastSeen'.

...

#define REAL_NAME_ATTRIBUTE_NAME	"realName"
#define REAL_NAME_ATTRIBUTE_INDEX	0
#define LAST_SEEN_ATTRIBUTE_NAME	"lastSeen"
#define LAST_SEEN_ATTRIBUTE_INDEX	1

	struct aws_handle *aws_dynamo;
	struct aws_dynamo_attribute attributes[] = {
		{
			/* Index: REAL_NAME_ATTRIBUTE_INDEX */
			.type = AWS_DYNAMO_STRING,
			.name = REAL_NAME_ATTRIBUTE_NAME,
			.name_len = strlen(REAL_NAME_ATTRIBUTE_NAME),
		},
		{
			/* Index: LAST_SEEN_ATTRIBUTE_INDEX */
			.type = AWS_DYNAMO_NUMBER,
			.name = LAST_SEEN_ATTRIBUTE_NAME,
			.name_len = strlen(LAST_SEEN_ATTRIBUTE_NAME),
			.value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		},
	};
	struct aws_dynamo_get_item_response *r = NULL;
	struct aws_dynamo_attribute *real_name;
	struct aws_dynamo_attribute *last_seen;

	const char *request = 
"{\
	\"TableName\":\"users\",\
	\"Key\":{\"\
		HashKeyElement\":{\"S\":\"jdoe\"}
	},\
	\"AttributesToGet\":[\""\
		REAL_NAME_ATTRIBUTE_NAME "\",\"" \
		LAST_SEEN_ATTRIBUTE_NAME \
	"\"]\
}";

	aws_dynamo = aws_init(aws_access_key_id, aws_secret_access_key);

	r = aws_dynamo_get_item(aws_dynamo, request, attributes, sizeof(attributes) / sizeof(attributes[0]));

	if (r == NULL) {
		return -1;
	}

	if (r->item.attributes == NULL) {
		/* No item found. */
		return;
	}

	real_name = &(r->item.attributes[REAL_NAME_ATTRIBUTE_INDEX]);
	last_seen = &(r->item.attributes[LAST_SEEN_ATTRIBUTE_INDEX]);

	/* prints: "John Doe was last seen at 1391883778." */
	printf("%s was last seen at %d.", real_name->value.string, last_seen->value.number.value.integer_val);

	aws_dynamo_free_get_item_response(r);

...

Notes
=====

In all cases the caller is responsible for creating the request json.  This
requires some understanding of the AWS DynamoDB API.  The most recent AWS
DynamoDB API is different from the one implemented here.  Documentation for the
DynamoDB API version that this ilbrary implements can be found here:

http://aws.amazon.com/archives/Amazon-DynamoDB

See "Docs: Amazon DynamoDB (API Version 2011-12-05)"

See section, "Operations in Amazon DynamoDB" starting on page 331 for details
on creating DynamoDB JSON requests.

In many cases the caller also provides a template for the structure where the
response will be written.  The then accesses the response attributes directly
using known array indices.

The response json is parsed in all cases and returned to the caller in a C
structure that must be free'd correctly when the caller is finished with it.

