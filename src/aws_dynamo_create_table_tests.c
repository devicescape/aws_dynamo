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
#include "aws_dynamo_create_table.h"

struct aws_dynamo_create_table_response *aws_dynamo_parse_create_table_response(const char *response, int response_len);

static void test_parse_create_table(void)
{
	struct aws_dynamo_create_table_response *r;
	const char *json = 
"{\"TableDescription\":{\
		\"CreationDateTime\":1.391273126865E9,\
		\"ItemCount\":0,\
		\"KeySchema\":{\
			\"HashKeyElement\":{\
				\"AttributeName\":\"id\",\
				\"AttributeType\":\"N\"\
			}\
		},\
		\"ProvisionedThroughput\":{\
			\"NumberOfDecreasesToday\":0,\
			\"ReadCapacityUnits\":1,\
			\"WriteCapacityUnits\":1\
		},\
		\"TableName\":\"aws_dynamo_test_table\",\
		\"TableSizeBytes\":0,\
		\"TableStatus\":\"CREATING\"\
	}\
}";

	r = aws_dynamo_parse_create_table_response(json, strlen(json));

	aws_dynamo_dump_create_table_response(r);
	aws_dynamo_free_create_table_response(r);
}

static void test_create_table(void)
{
	struct aws_dynamo_create_table_response *r;
	const char *request =
"{\
	\"TableName\":\"aws_dynamo_test_table\",\
	\"KeySchema\":{\
		\"HashKeyElement\":{\
			\"AttributeName\":\"id\",\
			\"AttributeType\":\"N\"\
		}},\
	\"ProvisionedThroughput\":{\
		\"ReadCapacityUnits\":1,\
		\"WriteCapacityUnits\":1\
	}\
}";
	struct aws_handle *aws_dynamo;

	aws_dynamo = aws_init(NULL, NULL);

	r = aws_dynamo_create_table(aws_dynamo, request);

	aws_dynamo_dump_create_table_response(r);
	aws_dynamo_free_create_table_response(r);
	aws_deinit(aws_dynamo);
}

int main(int argc, char *argv[])
{
	test_parse_create_table();
	test_create_table();

	return 0;
}
