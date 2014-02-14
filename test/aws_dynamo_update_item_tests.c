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
#include "aws_dynamo_update_item.h"

#include <assert.h>

struct aws_dynamo_update_item_response * aws_dynamo_parse_update_item_response(const char *response, int response_len, struct aws_dynamo_attribute *attributes, int num_attributes);

static void test_aws_dynamo_parse_update_item_response(const char *json, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	struct aws_dynamo_update_item_response *r;

	r = aws_dynamo_parse_update_item_response(json, strlen(json), attributes, num_attributes);

	aws_dynamo_dump_update_item_response(r);
	aws_dynamo_free_update_item_response(r);
}

static void test_parse_update_item_response_example(void)
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
/*
		{
		 .type = AWS_DYNAMO_NUMBER_SET,
		 .name = "AttributeName4",
		 .name_len = strlen("AttributeName4"),
		 },
*/
	};

	test_aws_dynamo_parse_update_item_response("{\"Attributes\":{\"AttributeName1\":{\"S\":\"AttributeValue1\"},\"AttributeName2\":{\"N\":\"2\"},\"AttributeName3\":{\"SS\":[\"AttributeValue3_1\", \"AttributeValue3_2\"]}},\"ConsumedCapacityUnits\":1}",
	     attributes, sizeof(attributes) / sizeof(attributes[0]));
}

static void test_parse_update_item_response_just_capacity(void)
{
	test_aws_dynamo_parse_update_item_response("{\"ConsumedCapacityUnits\":1}",
	     NULL, 0);
}

static void test_parse_update_item_response(void)
{
	test_parse_update_item_response_example();
	test_parse_update_item_response_just_capacity();
}

int main(int argc, char *argv[])
{
	test_parse_update_item_response();

	return 0;
}
