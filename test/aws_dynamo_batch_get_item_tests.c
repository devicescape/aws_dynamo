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

#include "aws_dynamo.h"

static void test_aws_dynamo_parse_batch_get_item_response(const char *json, struct aws_dynamo_batch_get_item_response_table *tables, int num_tables)
{
	struct aws_dynamo_batch_get_item_response *r;

	r = aws_dynamo_parse_batch_get_item_response(json, strlen(json), tables,
						     num_tables);

	aws_dynamo_dump_batch_get_item_response(r);
	aws_dynamo_free_batch_get_item_response(r);
}

static void test_parse_batch_get_item_response_example(void)
{
	struct aws_dynamo_attribute attributes[] = {
		{
		 .type = AWS_DYNAMO_STRING,
		 .name = "AttributeName1",
		 .name_len = strlen("AttributeName1"),
		 },
		{
		 .type = AWS_DYNAMO_NUMBER,
		 .name = "AttributeName2",
		 .name_len = strlen("AttributeName2"),
		 .value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		 },
		{
		 .type = AWS_DYNAMO_STRING_SET,
		 .name = "AttributeName3",
		 .name_len = strlen("AttributeName3"),
		 },
	};
	struct aws_dynamo_batch_get_item_response_table tables[] = {
		{
		 .name = "Table1",
		 .name_len = strlen("Table1"),
		 .num_attributes = sizeof(attributes) / sizeof(attributes[0]),
		 .attributes = attributes,
		 }
		,
		{
		 .name = "Table2",
		 .name_len = strlen("Table2"),
		 .num_attributes = sizeof(attributes) / sizeof(attributes[0]),
		 .attributes = attributes,
		 }
		,
	};

	test_aws_dynamo_parse_batch_get_item_response
	    ("{\"Responses\": {\"Table1\": {\"Items\": [{\"AttributeName1\": {\"S\":\"AttributeValue\"}, \"AttributeName2\": {\"N\":\"100\"}, \"AttributeName3\": {\"SS\":[\"AttributeValue\", \"AttributeValue\", \"AttributeValue\"]} }, {\"AttributeName1\": {\"S\": \"AttributeValue\"}, \"AttributeName2\": {\"N\": \"120\"}, \"AttributeName3\": {\"SS\": [\"AttributeValue\", \"AttributeValue\", \"AttributeValue\"]} }], \"ConsumedCapacityUnits\":1}, \"Table2\": {\"Items\": [{\"AttributeName1\": {\"S\":\"AttributeValue\"}, \"AttributeName2\": {\"N\":\"120\"}, \"AttributeName3\": {\"SS\":[\"AttributeValue\", \"AttributeValue\", \"AttributeValue\"]} }, {\"AttributeName1\": {\"S\": \"AttributeValue\"}, \"AttributeName2\": {\"N\": \"130\"}, \"AttributeName3\": {\"SS\": [\"AttributeValue\", \"AttributeValue\",\"AttributeValue\"]} }], \"ConsumedCapacityUnits\":1} },\"UnprocessedKeys\":{} }",
	     tables, sizeof(tables) / sizeof(tables[0]));
}

static void test_parse_batch_get_item_response(void)
{
	test_parse_batch_get_item_response_example();
}

int main(int argc, char *argv[])
{
	test_parse_batch_get_item_response();

	return 0;
}
