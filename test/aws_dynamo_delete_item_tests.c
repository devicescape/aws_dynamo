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

#include <string.h>
#include <unistd.h>
#include <assert.h>

#include "aws_dynamo.h"
#include "test_utils.h"

struct aws_dynamo_delete_item_response *aws_dynamo_parse_delete_item_response(const char *response,
	int response_len, struct aws_dynamo_attribute *attributes, int num_attributes);

static void test_parse_delete_item(void)
{
	struct aws_dynamo_delete_item_response *r;
	struct aws_dynamo_attribute attributes[] = {
		{
			.type = AWS_DYNAMO_STRING_SET,
			.name = "AttributeName3",
			.name_len = strlen("AttributeName3"),
		},
		{
			.type = AWS_DYNAMO_STRING,
			.name = "AttributeName2",
			.name_len = strlen("AttributeName2"),
		},
		{
			.type = AWS_DYNAMO_NUMBER,
			.name = "AttributeName1",
			.name_len = strlen("AttributeName1"),
		},
	};
	const char *json = \
"{\"Attributes\":\
	{\
		\"AttributeName3\":{\"SS\":[\
			\"AttributeValue3\",\
			\"AttributeValue4\",\
			\"Attribute Value5\"\
		]},\
		\"AttributeName2\":{\"S\":\"AttributeValue2\"},\
		\"AttributeName1\":{\"N\":\"1\"}\
	},\
	\"ConsumedCapacityUnits\":1\
}";

	r = aws_dynamo_parse_delete_item_response(json, strlen(json), attributes, sizeof(attributes) / sizeof(attributes[0]));

	aws_dynamo_free_delete_item_response(r);
}

static void test_delete_item(void)
{
	struct aws_handle *aws_dynamo;
	aws_dynamo_integer_t hash = 1;
	struct aws_dynamo_attribute attributes[] = {
		{
			.type = AWS_DYNAMO_NUMBER,
			.name = "hash",
			.name_len = strlen("hash"),
			.value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
			.value.number.value.integer_val = &hash,
		},
/*
		{
			.type = AWS_DYNAMO_STRING_SET,
			.name = "StringSet",
			.name_len = strlen("StringSet"),
		},
*/
		{
			.type = AWS_DYNAMO_STRING,
			.name = "String",
			.name_len = strlen("String"),
			.value.string = "Something"
		},
	};
	struct aws_dynamo_item item = {
		.attributes = attributes,
		.num_attributes = sizeof(attributes) / sizeof(attributes[0])
	};

	aws_dynamo = aws_init(NULL, NULL);
	create_test_table(aws_dynamo, "aws_dynamo_test_delete_item", "N", NULL);
	put_item(aws_dynamo, "aws_dynamo_test_delete_item", &item);

	aws_deinit(aws_dynamo);
}

int main(int argc, char *argv[])
{
	test_parse_delete_item();
	test_delete_item();

	return 0;
}
