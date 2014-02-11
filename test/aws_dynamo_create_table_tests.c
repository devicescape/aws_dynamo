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
#include "aws_dynamo_create_table.h"
#include "aws_dynamo_describe_table.h"
#include "aws_dynamo_delete_table.h"

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
	int write_units = 1;
	int read_units = 1;
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
	const char *describe_request = "{\
	\"TableName\":\"aws_dynamo_test_table\"\
}";
	struct aws_dynamo_describe_table_response *describe_r;
	const char *delete_request = "{\
	\"TableName\":\"aws_dynamo_test_table\"\
}";
	struct aws_handle *aws_dynamo;
	struct aws_dynamo_delete_table_response *delete_r;

	aws_dynamo = aws_init(NULL, NULL);

	assert(aws_dynamo != NULL);

	do {
		/* Make sure the table doesn't exist. */
		describe_r = aws_dynamo_describe_table(aws_dynamo, describe_request);

		aws_dynamo_dump_describe_table_response(describe_r);
		if (describe_r == NULL && aws_dynamo_get_errno(aws_dynamo) == AWS_DYNAMO_CODE_RESOURCE_NOT_FOUND_EXCEPTION) {
			/* Good, the table isn't there. */
			aws_dynamo_free_describe_table_response(describe_r);
			break;
		}

		if (describe_r->status == AWS_DYNAMO_TABLE_STATUS_ACTIVE) {
			delete_r = aws_dynamo_delete_table(aws_dynamo, delete_request);
			aws_dynamo_dump_delete_table_response(delete_r);
			aws_dynamo_free_delete_table_response(delete_r);
		}

		aws_dynamo_free_describe_table_response(describe_r);
		sleep(20);

	} while (1);

	r = aws_dynamo_create_table(aws_dynamo, request);

	aws_dynamo_dump_create_table_response(r);
	aws_dynamo_free_create_table_response(r);

	sleep(1);

	do {
		/* Make sure the table exists and is active. */
		describe_r = aws_dynamo_describe_table(aws_dynamo, describe_request);
		assert(describe_r != NULL);
		if (describe_r->status != AWS_DYNAMO_TABLE_STATUS_ACTIVE) {
			aws_dynamo_free_describe_table_response(describe_r);
			sleep(20);
			continue;
		}
		assert(describe_r->write_units == write_units);
		assert(describe_r->read_units == read_units);
		aws_dynamo_free_describe_table_response(describe_r);
		break;

	} while (1);

	aws_deinit(aws_dynamo);
}

int main(int argc, char *argv[])
{
	test_parse_create_table();
	test_create_table();

	return 0;
}
