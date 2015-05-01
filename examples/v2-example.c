/*
 * Copyright (c) 2015 Devicescape Software, Inc.
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
#include <aws_dynamo/aws_dynamo.h>

static void example_v2_query(struct aws_handle *aws)
{
	const char *request = "\
{\
    \"IndexName\": \"bssid_index\",\
    \"ExpressionAttributeValues\": { \":bssid\":{\"S\":\"001122334455\"}},\
    \"KeyConditionExpression\": \"bssid = :bssid\",\
    \"TableName\": \"tdssp_bss\"\
}\
";
	const char *response;
	int response_len;

	if (aws_dynamo_layer1_request(aws, AWS_DYNAMO_V2_QUERY, request) == -1) {
		fprintf(stderr, "failure performnig query");
	} else {
		response = aws_dynamo_layer1_get_response(aws, &response_len);

		printf("%s", response);
	}
}

int main(int argc, char *argv[])
{
	struct aws_handle *aws = aws_init(NULL, NULL);

	if (aws == NULL) {
		printf("Could not initialize.\n");
		exit(1);
	}

	example_v2_query(aws);

	aws_deinit(aws);
	return 0;
}
