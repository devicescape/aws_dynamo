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

#include "aws_dynamo_utils.h"
#include "aws.h"

#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

char *aws_sigv4_create_canonical_request(const char *http_request_method,
					 const char *canonical_uri,
					 const char *canonical_query_string,
					 const char *canonical_headers,
					 const char *signed_headers,
					 const char *request_payload)
{
	SHA256_CTX ctx;
	unsigned char hashed_request_payload[SHA256_DIGEST_LENGTH];
	char *canonical_request;

	SHA256_Init(&ctx);
	// XXX: Can we avoid this strlen() call?  Or reuse the result?
	SHA256_Update(&ctx, request_payload, strlen(request_payload));
	SHA256_Final(hashed_request_payload, &ctx);
	/* TODO - Need to hex encode hash_payload, or is it already hex encoded? */

	if (asprintf(&canonical_request, "%s\n%s\n%s\n%s\n%s\n%s", http_request_method,
	     canonical_uri, canonical_query_string, canonical_headers,
	     signed_headers, hashed_request_payload) == -1) {
		Warnx("aws_dynamo_create_signature: failed to create message");
		return NULL;
	}

	return canonical_request;
}

char *aws_sigv4_create_string_to_sign(time_t request_date,
				      const char *region,
				      const char *canonical_request)
{
	struct tm tm;
	char iso8601_basic_date[32];
	char yyyy_mm_dd[16];
	unsigned char hashed_canonical_request[SHA256_DIGEST_LENGTH];
	SHA256_CTX ctx;
	char *string_to_sign;

	/* FIXME - can reuse iso8601_basic_date from amz_date header. */
	if (gmtime_r(&request_date, &tm) == NULL) {
		Warnx("aws_sigv4_create_string_to_sign: Failed to get time structure.");
		return NULL;
	}

	if (strftime(iso8601_basic_date, sizeof(iso8601_basic_date),
		     "%Y%m%dT%H%M%SZ", &tm) == 0) {
		Warnx("aws_sigv4_create_string_to_sign: Failed to format time.");
		return NULL;
	}

	/* FIXME - can use yyyymmdd constructed by caller. */
	if (strftime(yyyy_mm_dd, sizeof(yyyy_mm_dd), "%Y%m%d", &tm) == 0) {
		Warnx("aws_sigv4_create_string_to_sign: Failed to format date.");
		return NULL;
	}

	SHA256_Init(&ctx);
	SHA256_Update(&ctx, canonical_request, strlen(canonical_request));
	SHA256_Final(hashed_canonical_request, &ctx);

	if (asprintf(&string_to_sign,
	     "AWS4-HMAC-SHA256\n%s\n%s/%s/dynamodb/aws4_request\n%s",
	     iso8601_basic_date, yyyy_mm_dd, region,
	     hashed_canonical_request) == -1) {
		Warnx("aws_dynamo_create_signature: failed to create string to sign.");
		return NULL;
	}

	return string_to_sign;
}

static char *aws_sigv4_derive_signing_key(struct aws_handle *aws,
	const char *aws_secret_access_key,
	time_t date, const char *region, const char *service, const void **key,
	int *key_len)
{
	// TODO = implement this

	return NULL;
}

char *aws_sigv4_create_signature(struct aws_handle *aws,
	const char *aws_secret_access_key, const unsigned char *message)
{
	unsigned char md[EVP_MAX_MD_SIZE];
	unsigned int md_len;
	char *signature;
	size_t encoded_len;
	int message_len = strlen(message);
	const void *key = NULL;
	int key_len = 0;

	aws_sigv4_derive_signing_key(aws, aws_secret_access_key,
	0 /* FIXME date */, "us-east-1" /* FIXME - hard coded region */,
	"dynamodb" /* FIXME - hard coded region */, &key, &key_len);

	HMAC_Init_ex(&(aws->hmac_ctx), key, key_len, EVP_sha256(), NULL);
	HMAC_Update(&(aws->hmac_ctx), message, message_len);
	HMAC_Final(&(aws->hmac_ctx), md, &md_len);

	/* TODO - Don't use base64 encoding, use hex encoding. */	
	signature = NULL;// base64_encode(md, md_len, &encoded_len);

	if (signature == NULL) {
		return NULL;
	}

	/* remove newline. */
	signature[encoded_len - 1] = '\0';

	HMAC_CTX_cleanup(&(aws->hmac_ctx));

	return signature;
}

