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

#include <assert.h>

static void test_aws_dynamo_parse_query_response(const char *json,
	struct aws_dynamo_attribute *attributes, int num_attributes) {

	struct aws_dynamo_query_response *r;


	r = aws_dynamo_parse_query_response(json, strlen(json), attributes, num_attributes);

	aws_dynamo_dump_query_response(r);
	aws_dynamo_free_query_response(r);
}

static void test_parse_query_response_credential(void) {
	struct aws_dynamo_attribute attributes[] = {
		{
			.type = AWS_DYNAMO_NUMBER,
			.name = "credentialProviderId",
			.name_len = strlen("credentialProviderId"),
			.value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		},
		{
			.type = AWS_DYNAMO_STRING_SET,
			.name = "tokens",
			.name_len = strlen("tokens"),
		},
		{
			.type = AWS_DYNAMO_NUMBER,
			.name = "activeUntil",
			.name_len = strlen("activeUntil"),
			.value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		},
	};

	test_aws_dynamo_parse_query_response("{\"ConsumedCapacityUnits\":0.5,\"Count\":2,\"Items\":[{\"credentialProviderId\":{\"N\":\"4\"},\"tokens\":{\"SS\":[\"password=attpass\",\"username=attuser\"]}},{\"credentialProviderId\":{\"N\":\"26\"},\"tokens\":{\"SS\":[\"email=acmeemail\",\"password=acmepass\",\"username=acme,=user\"]},\"activeUntil\":{\"N\":\"456\"}}]}", attributes, sizeof(attributes) / sizeof(attributes[0]));
 
	/* Make some whitespace changes, remove ConsumedCapacityUnits. */
	test_aws_dynamo_parse_query_response("{\"Count\":2,\"Items\":[{\"credentialProviderId\":{\"N\":\"4\"},\n\n\n\"tokens\"\n\t:{\"SS\":[ \"password=attpass\", \"username=attuser\"]}},{\"credentialProviderId\":{\"N\":\"26\"},\"tokens\":{\"SS\":[\"email=acmeemail\",\"password=acmepass\",\"username=acme,=user\"]},\"activeUntil\":{\"N\":\"456\"}}]}", attributes, sizeof(attributes) / sizeof(attributes[0]));

	test_aws_dynamo_parse_query_response("{\"Count\":2,\"Items\":[{\"credentialProviderId\":{\"N\":\"4\"},\n\n\n\"tokens\"\n\t:{\"SS\":[ \"password=attpass\", \"username=attuser\"]}},{\"credentialProviderId\":{\"N\":\"26\"},\"tokens\":{\"SS\":[\"email=acmeemail\",\"password=acmepass\",\"username=acme,=user\"]},\"activeUntil\":{\"N\":\"456\"}}],\"LastEvaluatedKey\":{\"HashKeyElement\":{\"AttributeValue3\":\"S\"},\"RangeKeyElement\":{\"AttributeValue4\":\"N\"}},\"ConsumedCapacityUnits\":1}", attributes, sizeof(attributes) / sizeof(attributes[0]));
}
	
static void test_parse_query_response_network(void) {
	struct aws_dynamo_attribute attributes[] = {
		{
			.type = AWS_DYNAMO_NUMBER,
			.name = "defaultFlag",
			.name_len = strlen("defaultFlag"),
			.value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		},
		{
			.type = AWS_DYNAMO_NUMBER,
			.name = "portal",
			.name_len = strlen("portal"),
			.value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		},
		{
			.type = AWS_DYNAMO_NUMBER,
			.name = "serviceState",
			.name_len = strlen("serviceState"),
			.value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		},
	};

	test_aws_dynamo_parse_query_response("{\"ConsumedCapacityUnits\":0.5,\"Count\":1,\"Items\":[{\"defaultFlag\":{\"N\":\"0\"},\"portal\":{\"N\":\"0\"},\"serviceState\":{\"N\":\"3\"}}]}", attributes, sizeof(attributes) / sizeof(attributes[0]));
}

static void test_parse_query_response(void) {
	test_parse_query_response_credential();
	test_parse_query_response_network();
}

int main(int argc, char *argv[]) {

	test_parse_query_response();

	return 0;
}
