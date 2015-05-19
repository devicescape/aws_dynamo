/*
 * Copyright (c) 2012-2014 Devicescape Software, Inc.
 * This file is part of aws_dynamo, a C library for AWS DynamoDB.
 *
 * aws_dynamo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aws_dynamo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aws_dynamo.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _AWS_DYNAMO_H_
#define _AWS_DYNAMO_H_

#include "aws.h"

#ifdef  __cplusplus
extern "C" {
#endif

#define AWS_DYNAMO_DEFAULT_HOST "dynamodb.us-east-1.amazonaws.com"
#define AWS_DYNAMO_DEFAULT_REGION "us-east-1"

/* AWS DynamoDB HTTP headers names.  These are all lowercase
	to simplify the signature calculation. */
/* FIXME: Change these to just AWS, move into aws.h */
#define AWS_DYNAMO_AUTHORIZATION_HEADER	"authorization"
#define AWS_DYNAMO_DATE_HEADER	"x-amz-date"
#define AWS_DYNAMO_AUTH_HEADER	"x-amzn-authorization"
#define AWS_DYNAMO_TARGET_HEADER	"x-amz-target"
#define AWS_DYNAMO_TOKEN_HEADER	"x-amz-security-token"

/* Content-Type: value for DynamoDB requests */
#define AWS_DYNAMO_CONTENT_TYPE	"application/x-amz-json-1.0"

#define AWS_DYNAMO_BATCH_GET_ITEM	"DynamoDB_20111205.BatchGetItem"
#define AWS_DYNAMO_BATCH_WRITE_ITEM	"DynamoDB_20111205.BatchWriteItem"
#define AWS_DYNAMO_CREATE_TABLE		"DynamoDB_20111205.CreateTable"
#define AWS_DYNAMO_DELETE_ITEM		"DynamoDB_20111205.DeleteItem"
#define AWS_DYNAMO_DELETE_TABLE		"DynamoDB_20111205.DeleteTable"
#define AWS_DYNAMO_DESCRIBE_TABLE	"DynamoDB_20111205.DescribeTable"
#define AWS_DYNAMO_GET_ITEM			"DynamoDB_20111205.GetItem"
#define AWS_DYNAMO_LIST_TABLES		"DynamoDB_20111205.ListTables"
#define AWS_DYNAMO_PUT_ITEM			"DynamoDB_20111205.PutItem"
#define AWS_DYNAMO_QUERY				"DynamoDB_20111205.Query"
#define AWS_DYNAMO_SCAN					"DynamoDB_20111205.Scan"
#define AWS_DYNAMO_UPDATE_ITEM		"DynamoDB_20111205.UpdateItem"
#define AWS_DYNAMO_UPDATE_TABLE		"DynamoDB_20111205.UpdateTable"

#define AWS_DYNAMO_V2_BATCH_GET_ITEM	"DynamoDB_20120810.BatchGetItem"
#define AWS_DYNAMO_V2_BATCH_WRITE_ITEM	"DynamoDB_20120810.BatchWriteItem"
#define AWS_DYNAMO_V2_CREATE_TABLE		"DynamoDB_20120810.CreateTable"
#define AWS_DYNAMO_V2_DELETE_ITEM		"DynamoDB_20120810.DeleteItem"
#define AWS_DYNAMO_V2_DELETE_TABLE		"DynamoDB_20120810.DeleteTable"
#define AWS_DYNAMO_V2_DESCRIBE_TABLE	"DynamoDB_20120810.DescribeTable"
#define AWS_DYNAMO_V2_GET_ITEM			"DynamoDB_20120810.GetItem"
#define AWS_DYNAMO_V2_LIST_TABLES		"DynamoDB_20120810.ListTables"
#define AWS_DYNAMO_V2_PUT_ITEM			"DynamoDB_20120810.PutItem"
#define AWS_DYNAMO_V2_QUERY				"DynamoDB_20120810.Query"
#define AWS_DYNAMO_V2_SCAN					"DynamoDB_20120810.Scan"
#define AWS_DYNAMO_V2_UPDATE_ITEM		"DynamoDB_20120810.UpdateItem"
#define AWS_DYNAMO_V2_UPDATE_TABLE		"DynamoDB_20120810.UpdateTable"

#define AWS_DYNAMO_ACCESS_DENIED_EXCEPTION							"AccessDeniedException"
#define AWS_DYNAMO_CONDITIONAL_CHECK_FAILED_EXCEPTION				"ConditionalCheckFailedException"
#define AWS_DYNAMO_INCOMPLETE_SIGNATURE_EXCEPTION					"IncompleteSignatureException"
#define AWS_DYNAMO_LIMIT_EXCEEDED_EXCEPTION							"LimitExceededException"
#define AWS_DYNAMO_MISSING_AUTHENTICATION_TOKEN_EXCEPTION		"MissingAuthenticationTokenException"
#define AWS_DYNAMO_PROVISIONED_THROUGHPUT_EXCEEDED_EXCEPTION	"ProvisionedThroughputExceededException"
#define AWS_DYNAMO_RESOURCE_IN_USE_EXCEPTION 						"ResourceInUseException"
#define AWS_DYNAMO_RESOURCE_NOT_FOUND_EXCEPTION						"ResourceNotFoundException"
#define AWS_DYNAMO_THROTTLING_EXCEPTION								"ThrottlingException"
#define AWS_DYNAMO_VALIDATION_EXCEPTION								"ValidationException"
#define AWS_DYNAMO_INTERNAL_FAILURE										"InternalFailure"
#define AWS_DYNAMO_INTERNAL_SERVER_ERROR								"InternalServerError"
#define AWS_DYNAMO_SERVICE_UNAVAILABLE_EXCEPTION 					"ServiceUnavailableException"

enum {
	AWS_DYNAMO_CODE_UNKNOWN,
	AWS_DYNAMO_CODE_NONE,
	AWS_DYNAMO_CODE_ACCESS_DENIED_EXCEPTION,
	AWS_DYNAMO_CODE_CONDITIONAL_CHECK_FAILED_EXCEPTION,
	AWS_DYNAMO_CODE_INCOMPLETE_SIGNATURE_EXCEPTION,
	AWS_DYNAMO_CODE_LIMIT_EXCEEDED_EXCEPTION,
	AWS_DYNAMO_CODE_MISSING_AUTHENTICATION_TOKEN_EXCEPTION,
	AWS_DYNAMO_CODE_PROVISIONED_THROUGHPUT_EXCEEDED_EXCEPTION,
	AWS_DYNAMO_CODE_RESOURCE_IN_USE_EXCEPTION,
	AWS_DYNAMO_CODE_RESOURCE_NOT_FOUND_EXCEPTION,
	AWS_DYNAMO_CODE_THROTTLING_EXCEPTION,
	AWS_DYNAMO_CODE_VALIDATION_EXCEPTION,
	AWS_DYNAMO_CODE_INTERNAL_FAILURE,
	AWS_DYNAMO_CODE_INTERNAL_SERVER_ERROR,
	AWS_DYNAMO_CODE_SERVICE_UNAVAILABLE_EXCEPTION,
};

#define AWS_DYNAMO_JSON_RESPONSES			"Responses"
#define AWS_DYNAMO_JSON_ITEM					"Item"
#define AWS_DYNAMO_JSON_ITEMS					"Items"
#define AWS_DYNAMO_JSON_ATTRIBUTES			"Attributes"
#define AWS_DYNAMO_JSON_COUNT					"Count"
#define AWS_DYNAMO_JSON_SCANNED_COUNT		"ScannedCount"
#define AWS_DYNAMO_JSON_LAST_EVALUATED_KEY	"LastEvaluatedKey"
#define AWS_DYNAMO_JSON_LAST_EVALUATED_TABLE_NAME	"LastEvaluatedTableName"
#define AWS_DYNAMO_JSON_HASH_KEY_ELEMENT	"HashKeyElement"
#define AWS_DYNAMO_JSON_RANGE_KEY_ELEMENT	"RangeKeyElement"
#define AWS_DYNAMO_JSON_UNPROCESSED_ITEMS	"UnprocessedItems"
#define AWS_DYNAMO_JSON_UNPROCESSED_KEYS	"UnprocessedKeys"
#define AWS_DYNAMO_JSON_CONSUMED_CAPACITY	"ConsumedCapacityUnits"
#define AWS_DYNAMO_JSON_TYPE_STRING			"S"
#define AWS_DYNAMO_JSON_TYPE_STRING_SET	"SS"
#define AWS_DYNAMO_JSON_TYPE_NUMBER			"N"
#define AWS_DYNAMO_JSON_TYPE_NUMBER_SET	"NS"

#define AWS_DYNAMO_JSON_TABLE_DESCRIPTION "TableDescription"
#define AWS_DYNAMO_JSON_TABLE "Table"
#define AWS_DYNAMO_JSON_TABLE_NAMES "TableNames"
#define AWS_DYNAMO_JSON_CREATION_DATE_TIME "CreationDateTime"
#define AWS_DYNAMO_JSON_KEY_SCHEMA "KeySchema"
#define AWS_DYNAMO_JSON_HASHKEY_ELEMENT "HashKeyElement"
#define AWS_DYNAMO_JSON_ATTRIBUTE_NAME "AttributeName"
#define AWS_DYNAMO_JSON_ATTRIBUTE_TYPE "AttributeType"
#define AWS_DYNAMO_JSON_RANGE_KEY_ELEMENT "RangeKeyElement"
#define AWS_DYNAMO_JSON_PROVISIONED_THROUGHPUT "ProvisionedThroughput"
#define AWS_DYNAMO_JSON_READ_CAPACITY_UNITS "ReadCapacityUnits"
#define AWS_DYNAMO_JSON_WRITE_CAPACITY_UNITS "WriteCapacityUnits"
#define AWS_DYNAMO_JSON_LAST_INCREASE_DATE_TIME "LastIncreaseDateTime"
#define AWS_DYNAMO_JSON_LAST_DECREASE_DATE_TIME "LastDecreaseDateTime"
#define AWS_DYNAMO_JSON_TABLE_NAME "TableName"
#define AWS_DYNAMO_JSON_TABLE_STATUS "TableStatus"
#define AWS_DYNAMO_JSON_ITEM_COUNT "ItemCount"
#define AWS_DYNAMO_JSON_TABLE_SIZE_BYTES "TableSizeBytes"
#define AWS_DYNAMO_JSON_NUMBER_OF_DECREASES_TODAY "NumberOfDecreasesToday"

#define AWS_DYNAMO_JSON_TABLE_STATUS_CREATING	"CREATING"
#define AWS_DYNAMO_JSON_TABLE_STATUS_ACTIVE		"ACTIVE"
#define AWS_DYNAMO_JSON_TABLE_STATUS_DELETING	"DELETING"
#define AWS_DYNAMO_JSON_TABLE_STATUS_UPDATING	"UPDATING"

    /*FIXME: Rename*/
#define AWS_DYNAMO_VALCMP(s, val, len) (len == strlen(s) && \
														strncmp(val, s, len) == 0)

#define AWS_DYNAMO_VALCASECMP(s, val, len) (len == strlen(s) && \
														strncasecmp(val, s, len) == 0)


enum aws_dynamo_table_status {
	AWS_DYNAMO_TABLE_STATUS_CREATING = 0,
	AWS_DYNAMO_TABLE_STATUS_ACTIVE,
	AWS_DYNAMO_TABLE_STATUS_DELETING,
	AWS_DYNAMO_TABLE_STATUS_UPDATING,
};

enum aws_dynamo_attribute_type {
	/* The basic dynamo DB types. */
	AWS_DYNAMO_STRING = 0,
	AWS_DYNAMO_STRING_SET = 1,
	AWS_DYNAMO_NUMBER = 2,
	AWS_DYNAMO_NUMBER_SET = 3,
};

enum aws_dynamo_number_type {
	/* The C representation for DynamoDB number in question.
		XXX: This can be confusing, maybe need a better name for these.
		for example AWS_DYNAMO_NUMBER_CTYPE_INTEGER? And change the
		enum too to aws_dynamo_number_c_type? */
	AWS_DYNAMO_NUMBER_INTEGER = 0,
	AWS_DYNAMO_NUMBER_DOUBLE = 1,
};

/* XXX: The indexes in this array must correspond to
	to the aws_dynamo_attribute_type above. */
/* XXX: maybe make this a function instead. */
extern const char *aws_dynamo_attribute_types[];

struct aws_dynamo_string_set {
	int num_strings;
	char **strings;
};

struct aws_dynamo_number_set {
	int n;
	/* This is the C type for all numbers in this set. */
	enum aws_dynamo_number_type type;
	struct aws_dynamo_number *numbers;
};

/* XXX: A long long int isn't big enough in general but it
   is big enough for our intended use. */
typedef signed long long int aws_dynamo_integer_t;

/* XXX: A long double isn't big enough in general but it
   is big enough for our intended use. */
typedef long double aws_dynamo_double_t;

struct aws_dynamo_number {
	/* A DynamoDB number can have up to 38 digits precision
		and can be between 10^-128 to 10^+126. This structure
		is used to represent these numbers as best we can. */

	enum aws_dynamo_number_type type;
	union {
		aws_dynamo_integer_t *integer_val;

		aws_dynamo_double_t *double_val;

	} value;
};

struct aws_dynamo_attribute {

	/* The name of the attribute. */
	const char *name;

	/* The length of the attribute name.  This is used by an optimization
		to quickly determine an attribute name match. */
	int name_len;

	/* The DynamoDB type: Number, Number Set, String, String Set. */
	enum aws_dynamo_attribute_type type;

	/* The value for the attribute. 'type' above determines
		which entry in the union is used. */
	union {
		char *string;
		struct aws_dynamo_number number;
		struct aws_dynamo_string_set string_set;
		struct aws_dynamo_number_set number_set;
	} value;

};

/* A single item, a DynamoDB row. */
struct aws_dynamo_item {
	int num_attributes;
	struct aws_dynamo_attribute *attributes;
};

struct aws_dynamo_key {
	char *type;
	char *value;
};

int aws_dynamo_json_get_int(const unsigned char *val, size_t len, int *i);
int aws_dynamo_json_get_double(const char *val, size_t len, double *d);
int aws_dynamo_json_get_type(const unsigned char *val, size_t len, enum aws_dynamo_attribute_type *type);
int aws_dynamo_json_get_table_status(const unsigned char *val, size_t len, enum aws_dynamo_table_status *status);

int aws_dynamo_request(struct aws_handle *aws, const char *target, const char *body);

void aws_dynamo_free_attributes(struct aws_dynamo_attribute *attributes,
	int num_attributes);

void aws_dynamo_dump_attributes(struct aws_dynamo_attribute *attributes,
	int num_attributes);

int aws_dynamo_parse_attribute_value(struct aws_dynamo_attribute *attribute, const unsigned char *val,  size_t len);

/* Begin public interface. */

void aws_dynamo_set_max_retries(struct aws_handle *aws, int dynamo_max_retries);

void aws_dynamo_set_https(struct aws_handle *aws, int https);

void aws_dynamo_set_https_certificate_file(struct aws_handle *aws, const char *certfile);

/**
 * aws_dynamo_set_endpoint() - Set the endpoint to use for DynamoDB requests.
 * @aws:	Library handle.
 * @host:	Endpoint hostname (ex. 'dynamodb.us-west-2.amazonaws.com').
 * @region:	Endpoint region (ex. 'us-east-1').
 *
 * The endpoint is the host to use for DynamoDB requests.  See the list
 * of supported endpoints here:
 * http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region
 *
 * In addition to the AWS hosted endpoints described above this setting
 * can be used to configure the library to use a local DynamoDB local endpoint.
 *
 * Return: 0 on success, -1 on failure to allocate a copy of @host.
 */
int aws_dynamo_set_endpoint(struct aws_handle *aws, const char *host, const char *region);

/**
 * aws_dynamo_set_port() - Set the TCP port to use for DynamoDB requests.
 * @aws:	Library handle.
 * @port:	port number (80, 443, 8080, etc), or 0 to use the default port
 *
 * By default the The TCP is 80 for unencrypted (http) traffic and 443 when
 * traffic is encruptedi, i.e. https, see aws_dynamo_set_https().
 */
void aws_dynamo_set_port(struct aws_handle *aws, int port);

char *aws_dynamo_get_message(struct aws_handle *aws);
int aws_dynamo_get_errno(struct aws_handle *aws);

struct aws_dynamo_item *aws_dynamo_copy_item(struct aws_dynamo_item *item);

void aws_dynamo_free_item(struct aws_dynamo_item *item);

#include "aws_dynamo_batch_get_item.h"
#include "aws_dynamo_batch_write_item.h"
#include "aws_dynamo_create_table.h"
#include "aws_dynamo_delete_item.h"
#include "aws_dynamo_delete_table.h"
#include "aws_dynamo_describe_table.h"
#include "aws_dynamo_get_item.h"
#include "aws_dynamo_list_tables.h"
#include "aws_dynamo_put_item.h"
#include "aws_dynamo_query.h"
#include "aws_dynamo_scan.h"
#include "aws_dynamo_update_item.h"
#include "aws_dynamo_update_table.h"

int aws_dynamo_layer1_request(struct aws_handle *aws, const char *target, const char *body);

const char *aws_dynamo_layer1_get_response(struct aws_handle *aws, int *response_len);

#ifdef  __cplusplus
}
#endif

#endif /* _AWS_DYNAMO_H_ */
