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

#define _GNU_SOURCE
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#include "aws_dynamo.h"

int aws_dynamo_item_snprintf(char *buf, size_t buflen, struct aws_dynamo_attribute *attributes,
	int num_attributes) {
	int j;
	int n = 0;
	int rv;

	for (j = 0; j < num_attributes; j++) {
		struct aws_dynamo_attribute *attribute;
		
		attribute = &(attributes[j]);
		switch (attribute->type) {
			case AWS_DYNAMO_STRING: {
				if (attribute->value.string != NULL) {
					rv = snprintf(buf + n, buflen - n,
						"%s\"%s\":{\""AWS_DYNAMO_JSON_TYPE_STRING"\":\"%s\"}",
						j == 0 ? "" : ",",
						attribute->name,
						attribute->value.string);
					if (rv < 0 || rv > buflen - n) {
						printf("Truncated buffer at attribute %s.", attribute->name);
						return -1;
					}
					n += rv;
				} else {
					printf("Attribute %s has NULL value", attribute->name);
					return -1;
				}
				break;
			}
			case AWS_DYNAMO_NUMBER: {
				switch (attribute->value.number.type) {
					case AWS_DYNAMO_NUMBER_INTEGER: {
						if (attribute->value.number.value.integer_val != NULL) {
							rv = snprintf(buf + n, buflen - n,
								"%s\"%s\":{\""AWS_DYNAMO_JSON_TYPE_NUMBER"\":\"%lld\"}",
								j == 0 ? "" : ",",
								attribute->name,
								*(attribute->value.number.value.integer_val));
							if (rv < 0 || rv > buflen - n) {
								printf("Truncated buffer at attribute %s.", attribute->name);
								return -1;
							}
							n += rv;
						} else {
							printf("Attribute %s has NULL value", attribute->name);
							return -1;
						}
						break;
					}
					case AWS_DYNAMO_NUMBER_DOUBLE: {
						if (attribute->value.number.value.double_val != NULL) {
							rv = snprintf(buf + n, buflen - n,
								"%s\"%s\":{\""AWS_DYNAMO_JSON_TYPE_NUMBER"\":\"%Lf\"}",
								j == 0 ? "" : ",",
								attribute->name,
								*(attribute->value.number.value.double_val));
							if (rv < 0 || rv > buflen - n) {
								printf("Truncated buffer at attribute %s.", attribute->name);
								return -1;
							}
							n += rv;
						} else {
							printf("Attribute %s has NULL value", attribute->name);
							return -1;
						}
						break;
					}
					default: {
						printf("Unknown number type %d",
							attribute->value.number.type);
						return -1;
					}
				}

				break;
			}
			case AWS_DYNAMO_STRING_SET: {
				if (attribute->value.string_set.strings != NULL) {
					int i;
					rv = snprintf(buf + n, buflen - n,
						"%s\"%s\":{\""AWS_DYNAMO_JSON_TYPE_STRING_SET"\":[",
						j == 0 ? "" : ",",
						attribute->name);
					if (rv < 0 || rv > buflen - n) {
						printf("Truncated buffer at attribute %s.", attribute->name);
						return -1;
					}
					n += rv;
					for (i = 0; i < attribute->value.string_set.num_strings; i++) {
						rv = snprintf(buf + n, buflen - n,
							"%s\"%s\"%s",
							i == 0 ? "" : ",",
							attribute->value.string_set.strings[i],
							i == attribute->value.string_set.num_strings - 1 ? "]}" : "");
						if (rv < 0 || rv > buflen - n) {
							printf("Truncated buffer at attribute %s.", attribute->name);
							return -1;
						}
						n += rv;
					}
				} else {
					printf("Attribute %s is NULL", attribute->name);
					return -1;
				}
				break;
			}
			case AWS_DYNAMO_NUMBER_SET: {
				/* FIXME: Unimplemented. */
				printf("aws_dynamo_dump_attributes - Number sets not implemented.");
				break;
			}
			default: {
				printf("aws_dynamo_dump_attributes - Unknown type %d",
					attribute->type);
				break;
			}
		}
	}
	return n;
}

int put_item(struct aws_handle *aws_dynamo, const char *table_name, struct aws_dynamo_item *item) {
	char put_request[BUFSIZ];
	struct aws_dynamo_put_item_response *put_r;
	int n = 0;
	int rv;

	rv = snprintf(put_request + n, sizeof(put_request) - n, "{\"TableName\":\"%s\",\"Item\":{", table_name);
	if (rv < 0 || rv > sizeof(put_request) - n) {
		return -1;
	}
	n += rv;
	rv = aws_dynamo_item_snprintf(put_request + n, sizeof(put_request) - n, item->attributes, item->num_attributes);
	if (rv < 0 || rv > sizeof(put_request) - n) {
		return -1;
	}
	n += rv;

	rv = snprintf(put_request + n, sizeof(put_request) - n, "}}");
	if (rv < 0 || rv > sizeof(put_request) - n) {
		return -1;
	}
	n += rv;
	
	put_r = aws_dynamo_put_item(aws_dynamo, put_request, NULL, 0);
	if (put_r == NULL) {
		return -1;
	}
	aws_dynamo_free_put_item_response(put_r);
	return 0;
}

int get_table_status(struct aws_handle *aws_dynamo, const char *table_name)
{
	const char *describe_request_format = "{\
	\"TableName\":\"%s\"\
}";
	char *describe_request = NULL;
	struct aws_dynamo_describe_table_response *describe_r;
	int rv;

	assert(asprintf(&describe_request, describe_request_format, table_name)
	       != -1);

	describe_r = aws_dynamo_describe_table(aws_dynamo, describe_request);
	if (describe_r == NULL) {
		free(describe_request);
		return -1;
	}
	rv = describe_r->status;
	aws_dynamo_free_describe_table_response(describe_r);
	free(describe_request);
	return rv;
}

void wait_for_table(struct aws_handle *aws_dynamo, const char *table_name)
{
	int status;

	status = get_table_status(aws_dynamo, table_name);

	while (status != AWS_DYNAMO_TABLE_STATUS_ACTIVE &&
	       status != AWS_DYNAMO_TABLE_STATUS_CREATING) {
		sleep(10);
		status = get_table_status(aws_dynamo, table_name);
	}
}

void create_test_table(struct aws_handle *aws_dynamo, const char *table_name,
		       const char *hash_key_type, const char *range_key_type)
{
	struct aws_dynamo_create_table_response *r;
	const char *create_request_hash_format = "{\
	\"TableName\":\"%s\",\
	\"KeySchema\":{\
		\"HashKeyElement\":{\
			\"AttributeName\":\"hash\",\
			\"AttributeType\":\"%s\"\
		}\
	},\
	\"ProvisionedThroughput\":{\
		\"ReadCapacityUnits\":1,\
		\"WriteCapacityUnits\":1\
	}\
}";
	const char *create_request_hash_range_format = "{\
	\"TableName\":\"%s\",\
	\"KeySchema\":{\
		\"HashKeyElement\":{\
			\"AttributeName\":\"hash\",\
			\"AttributeType\":\"%s\"\
		},\
		\"RangeKeyElement\":{\
			\"AttributeName\":\"range\",\
			\"AttributeType\":\"%s\"\
		}\
	},\
	\"ProvisionedThroughput\":{\
		\"ReadCapacityUnits\":1,\
		\"WriteCapacityUnits\":1\
	}\
}";
	char *create_request = NULL;
	int status;

	status = get_table_status(aws_dynamo, table_name);


	if (status != AWS_DYNAMO_TABLE_STATUS_ACTIVE &&
	    status != AWS_DYNAMO_TABLE_STATUS_CREATING) {
		if (hash_key_type && !range_key_type) {
			assert(asprintf(&create_request, create_request_hash_format, table_name, hash_key_type) != -1);
		} else if (hash_key_type && range_key_type) {
			assert(asprintf(&create_request, create_request_hash_range_format, table_name, hash_key_type, range_key_type) != -1);
		}

		r = aws_dynamo_create_table(aws_dynamo, create_request);

		assert(r != NULL);
		free(create_request);
		aws_dynamo_free_create_table_response(r);
	}

	wait_for_table(aws_dynamo, table_name);

}


