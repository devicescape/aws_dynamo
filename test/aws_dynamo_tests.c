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
#line 0 "aws_dynamo.c"
#include "aws_dynamo.c"
#line 3 "aws_dynamo_tests.c"

#include <assert.h>

static void test_aws_dynamo_parse_get_device_response(const char *json, const char *expected_deviceKey,
	const char *expected_user, int expected_state) {
	char *deviceKey = NULL;
	char *user = NULL;
	int state = -1;
	int rv;

	rv = aws_dynamo_parse_get_device_response(json, strlen(json), &deviceKey, &user, &state);

	if (expected_deviceKey == NULL) {
		assert(deviceKey == NULL);
	} else {
		assert(strcmp(deviceKey, expected_deviceKey) == 0);
	}

	if (expected_user == NULL) {
		assert(user == NULL);
	} else {
		assert(strcmp(user, expected_user) == 0);
	}

	assert(expected_state == state);

	free(deviceKey);
	free(user);
}

static void test_aws_dynamo_parse_get_credentials_response(const char *json,
	struct aws_dynamo_get_credentials_response *expected_response) {
	int i;
	struct aws_dynamo_get_credentials_response *response;

	response = aws_dynamo_parse_get_credentials_response(json, strlen(json));

	if (expected_response == NULL) {
		assert(response == NULL);
		return;
	}

	assert(response != NULL);

	assert(response->num_credentials == expected_response->num_credentials);

	for (i = 0; i < response->num_credentials; i++) {
		struct aws_dynamo_get_credentials_credential *expected_credential;
		struct aws_dynamo_get_credentials_credential *credential;
		int j;
	
		expected_credential = &(expected_response->credentials[i]);	
		credential = &(response->credentials[i]);

		assert(credential->credentialProviderId == expected_credential->credentialProviderId);
		assert(credential->activeUntil == expected_credential->activeUntil);
		assert(credential->num_tokens == expected_credential->num_tokens);

		for (j = 0; j < credential->num_tokens; j++) {
			struct token *expected_token;
			struct token *token;

			expected_token = &(expected_credential->tokens[j]);
			token = &(credential->tokens[j]);

			assert(strcmp(token->name, expected_token->name) == 0);
			assert(strcmp(token->value, expected_token->value) == 0);
		}	

	}

	aws_dynamo_free_get_credentials_response(response);


}

static void test_parse_get_device_response(void) {

	/* A typical response. */
	test_aws_dynamo_parse_get_device_response("{\"ConsumedCapacityUnits\":0.5,\"Item\":{\"deviceKey\":{\"S\":\"v1+5ue7Ic6xm2D+78kYhF5gZt0Sr0zHtTCw8x4+QkDrVhe3bcrAt10DiioX0hkAsXVSuj4cjOYODH6inmKk1FtxA64IsI41zDBkNgQLs1b9VMbQzWiWWI+p8hkBgUahT0P7br9PRuJwoZVViBvogfFBrwAyX0ExNzD4Y3u/+v3s=\"},\"state\":{\"N\":\"1\"},\"user\":{\"S\":\"foo@example.com\"}}}", "v1+5ue7Ic6xm2D+78kYhF5gZt0Sr0zHtTCw8x4+QkDrVhe3bcrAt10DiioX0hkAsXVSuj4cjOYODH6inmKk1FtxA64IsI41zDBkNgQLs1b9VMbQzWiWWI+p8hkBgUahT0P7br9PRuJwoZVViBvogfFBrwAyX0ExNzD4Y3u/+v3s=", "foo@example.com", 1);

	/* A more minimal possible response with only the attributes we care about
		with some whitespace changes. */
	test_aws_dynamo_parse_get_device_response("{\"Item\":{\"deviceKey\"\n:{ \"S\":\"key\"},  \"state\":  {\"N\":\"2\"},  \"user\":  {\"S\": \"user\"}}}", "key", "user", 2);
	/* Now with a different order for the items. */
	test_aws_dynamo_parse_get_device_response("{\"Item\":{ \"state\":  {\"N\":\"2\"},   \"deviceKey\"\n:{ \"S\":\"key\"}, \"user\":  {\"S\": \"user\"}}}", "key", "user", 2);
	test_aws_dynamo_parse_get_device_response("{\"Item\":{\"user\":  {\"S\": \"user\"},  \"state\":  {\"N\":\"2\"},   \"deviceKey\"\n:{ \"S\":\"key\"}}}", "key", "user", 2);

	/* Response with no state. */
	test_aws_dynamo_parse_get_device_response("{\"Item\":{\"deviceKey\":{\"S\":\"key\"},\"user\":{\"S\":\"user\"}}}", "key", "user", -1);

	/* Response with no user. */
	test_aws_dynamo_parse_get_device_response("{ \"Item\":\n{\"deviceKey\": {\"S\": \"key\"},\"state\" :{\"N\":\"2\"}}}", "key", NULL, 2);
	/* Response with no key. */
	test_aws_dynamo_parse_get_device_response("{\"Item\":{\"state\"\n:{\"N\":\"2\"},\"user\":{\"S\":\"user\"}\n}\n}", NULL, "user", 2);

	/* Response with nothing. */
	test_aws_dynamo_parse_get_device_response("{}", NULL, NULL, -1);
	test_aws_dynamo_parse_get_device_response("{\"ConsumedCapacityUnits\":0.5}", NULL, NULL, -1);

}

static void test_parse_get_credentials_response_1(void) {
	struct token att_tokens[] = {
		{ .name = "password", .value = "attpass" },
		{ .name = "username", .value = "attuser" },
	};
	struct token acme_tokens[] = {
		{ .name = "email", .value = "acmeemail" },
		{ .name = "password", .value = "acmepass" },
		{ .name = "username", .value = "acme,=user" },
	};
	struct aws_dynamo_get_credentials_credential expected_credentials[] = {
		{
			.credentialProviderId = 4,
			.num_tokens = sizeof(att_tokens) / sizeof(att_tokens[0]),
			.tokens = att_tokens,
			.activeUntil = 0,
		},
		{
			.credentialProviderId = 26,
			.num_tokens = sizeof(acme_tokens) / sizeof(acme_tokens[0]),
			.tokens = acme_tokens,
			.activeUntil = 456,
		},
	};
	struct aws_dynamo_get_credentials_response expected_response = {
		.num_credentials = 2,
		.credentials =  expected_credentials,
	};
	
	/*
		{
		"ConsumedCapacityUnits":0.5,
		"Count":2,
		"Items":
			[{
				"credentialProviderId":{"N":"4"},
				"tokens":{"SS":
					[
						"password=attpass",
						"username=attuser"
					]}
			},{
				"credentialProviderId":{"N":"26"},
				"tokens":{"SS":
					[
						"email=acmeemail",
						"password=acmepass",
						"username=acme,=user"
					]},
				"activeUntil":{"N":"456"}
			}]
		}
	*/

	test_aws_dynamo_parse_get_credentials_response("{\"ConsumedCapacityUnits\":0.5,\"Count\":2,\"Items\":[{\"credentialProviderId\":{\"N\":\"4\"},\"tokens\":{\"SS\":[\"password=attpass\",\"username=attuser\"]}},{\"credentialProviderId\":{\"N\":\"26\"},\"tokens\":{\"SS\":[\"email=acmeemail\",\"password=acmepass\",\"username=acme,=user\"]},\"activeUntil\":{\"N\":\"456\"}}]}", &expected_response);

	/* Make some whitespace changes, remove ConsumedCapacityUnits. */
	test_aws_dynamo_parse_get_credentials_response("{\"Count\":2,\"Items\":[{\"credentialProviderId\":{\"N\":\"4\"},\n\n\n\"tokens\"\n\t:{\"SS\":[ \"password=attpass\", \"username=attuser\"]}},{\"credentialProviderId\":{\"N\":\"26\"},\"tokens\":{\"SS\":[\"email=acmeemail\",\"password=acmepass\",\"username=acme,=user\"]},\"activeUntil\":{\"N\":\"456\"}}]}", &expected_response);
}

static void test_parse_get_credentials_response_2(void) {
	struct aws_dynamo_get_credentials_response expected_response = { 0 };
	test_aws_dynamo_parse_get_credentials_response("{}", &expected_response);
}

static void test_parse_get_credentials_response(void) {
	test_parse_get_credentials_response_1();
	test_parse_get_credentials_response_2();
}

int main(int argc, char *argv[]) {

#ifdef DEBUG_ENABLED
	err_verbose = err_debug = 1;
#endif
	error_init("mydns", LOG_DAEMON);

	test_parse_get_device_response();

	test_parse_get_credentials_response();

	return 0;
}
