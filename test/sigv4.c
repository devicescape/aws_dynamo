/*
 * Copyright (c) 2014 Devicescape Software, Inc.
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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "aws_sigv4.h"

void test_aws_sigv4_create_hashed_canonical_request(void) {
	char *hashed_canonical_request;

	hashed_canonical_request = aws_sigv4_create_hashed_canonical_request("POST",
	"/", "",
	"content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:iam.amazonaws.com\nx-amz-date:20110909T233600Z\n",
	"content-type;host;x-amz-date",
	"Action=ListUsers&Version=2010-05-08");

	assert(strcmp(hashed_canonical_request, "3511de7e95d28ecd39e9513b642aee07e54f4941150d8df8bf94b328ef7e55e2") == 0);
	free(hashed_canonical_request);
}

void test_aws_sigv4_create_string_to_sign(void) {
	char *string_to_sign;

	string_to_sign = aws_sigv4_create_string_to_sign("20110909T233600Z",
				      "20110909",
				      "us-east-1",
				      "iam",
				      "3511de7e95d28ecd39e9513b642aee07e54f4941150d8df8bf94b328ef7e55e2");

	assert(strcmp(string_to_sign, "AWS4-HMAC-SHA256\n20110909T233600Z\n20110909/us-east-1/iam/aws4_request\n3511de7e95d28ecd39e9513b642aee07e54f4941150d8df8bf94b328ef7e55e2") == 0);
	free(string_to_sign);
}

void test_aws_sigv4_derive_signing_key(void) {
	unsigned char *key;
	int key_len;
	int i;
	char buf[BUFSIZ];
	int n = 0;
	
	assert(aws_sigv4_derive_signing_key(
		"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
		"20110909", "us-east-1", "iam", &key, &key_len) == 0);

	for (i = 0; i < key_len; i++) {
		n += sprintf(buf + n, "%d ", key[i]);
	}
	assert(strcmp(buf, "152 241 216 137 254 196 244 66 26 220 82 43 171 12 225 248 46 105 41 194 98 237 21 229 169 76 144 239 209 227 176 231 ") == 0);
}

void test_aws_sigv4_create_signature() {
	char *sig;

	sig = aws_sigv4_create_signature(
		"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
		"20110909", "us-east-1", "iam",
		"AWS4-HMAC-SHA256\n20110909T233600Z\n20110909/us-east-1/iam/aws4_request\n3511de7e95d28ecd39e9513b642aee07e54f4941150d8df8bf94b328ef7e55e2");

	assert(strcmp(sig, "ced6826de92d2bdeed8f846f0bf508e8559e98e4b0199114b84c54174deb456c") == 0);
	free(sig);
}

int main(int argc, char *argv[]) {
	test_aws_sigv4_create_hashed_canonical_request();
	test_aws_sigv4_create_string_to_sign();
	test_aws_sigv4_derive_signing_key();
	test_aws_sigv4_create_signature();
	return 0;
}

