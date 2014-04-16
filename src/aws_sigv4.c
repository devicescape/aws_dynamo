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

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

char *aws_sigv4_create_hashed_canonical_request(const char *http_request_method,
					 const char *canonical_uri,
					 const char *canonical_query_string,
					 const char *canonical_headers,
					 const char *signed_headers,
					 const char *request_payload)
{
	SHA256_CTX ctx;
	unsigned char hash[SHA256_DIGEST_LENGTH];
	int i;
	unsigned char hex_hash[SHA256_DIGEST_LENGTH * 2 + 1];
	char *canonical_request;

	SHA256_Init(&ctx);
	SHA256_Update(&ctx, request_payload, strlen(request_payload));
	SHA256_Final(hash, &ctx);

	for (i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		sprintf(hex_hash + i * 2, "%.2x", hash[i]);
	}

	if (asprintf(&canonical_request, "%s\n%s\n%s\n%s\n%s\n%s", http_request_method,
	     canonical_uri, canonical_query_string, canonical_headers,
	     signed_headers, hex_hash) == -1) {
		Warnx("aws_dynamo_create_signature: failed to create message");
		return NULL;
	}

	SHA256_Init(&ctx);
	SHA256_Update(&ctx, canonical_request, strlen(canonical_request));
	SHA256_Final(hash, &ctx);

	free(canonical_request);

	for (i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		sprintf(hex_hash + i * 2, "%.2x", hash[i]);
	}

	return strdup(hex_hash);
}

char *aws_sigv4_create_string_to_sign(char *iso8601_basic_date,
				      char *yyyy_mm_dd,
				      const char *region,
				      const char *service,
				      const char *hashed_canonical_request)
{
	char *string_to_sign;

	if (asprintf(&string_to_sign,
	     "AWS4-HMAC-SHA256\n%s\n%s/%s/%s/aws4_request\n%s",
	     iso8601_basic_date, yyyy_mm_dd, region, service,
	     hashed_canonical_request) == -1) {
		Warnx("aws_dynamo_create_signature: failed to create string to sign.");
		return NULL;
	}

	return string_to_sign;
}

int aws_sigv4_derive_signing_key(
	const char *aws_secret_access_key, const char *yyyy_mm_dd,
	const char *region, const char *service, unsigned char **key /* OUT */,
	int *key_len /* OUT */)
{
	unsigned char md[EVP_MAX_MD_SIZE];
	unsigned int md_len;
	unsigned char date_key[128];
	int n;
	HMAC_CTX hmac_ctx;

	HMAC_CTX_init(&hmac_ctx);

	n = snprintf(date_key, sizeof(date_key), "AWS4%s",
		aws_secret_access_key);

	if (n < 0 || n > sizeof(date_key)) {
		Warnx("aws_sigv4_derive_signing_key: did not create date key.");
		return -1;
	}

	HMAC_Init_ex(&hmac_ctx, date_key, strlen(date_key), EVP_sha256(), NULL);
	HMAC_Update(&hmac_ctx, yyyy_mm_dd, strlen(yyyy_mm_dd));
	HMAC_Final(&hmac_ctx, md, &md_len);
	
	HMAC_Init_ex(&hmac_ctx, md, md_len, EVP_sha256(), NULL);
	HMAC_Update(&hmac_ctx, region, strlen(region));
	HMAC_Final(&hmac_ctx, md, &md_len);
	
	HMAC_Init_ex(&hmac_ctx, md, md_len, EVP_sha256(), NULL);
	HMAC_Update(&hmac_ctx, service, strlen(service));
	HMAC_Final(&hmac_ctx, md, &md_len);
	
	HMAC_Init_ex(&hmac_ctx, md, md_len, EVP_sha256(), NULL);
	HMAC_Update(&hmac_ctx, "aws4_request", strlen("aws4_request"));
	HMAC_Final(&hmac_ctx, md, &md_len);
	
	HMAC_CTX_cleanup(&hmac_ctx);

	*key = malloc(md_len);
	if (*key == NULL) {
		Warnx("aws_sigv4_derive_signing_key: key alloc failed.");
		return -1;
	}
	memcpy(*key, md, md_len);
	*key_len = md_len;
	return 0;
}

char *aws_sigv4_create_signature(
	const char *aws_secret_access_key, const char *yyyy_mm_dd,
	const char *region, const char *service,
	const unsigned char *message)
{
	HMAC_CTX hmac_ctx;
	unsigned char md[EVP_MAX_MD_SIZE];
	unsigned int md_len;
	char *signature;
	int message_len = strlen(message);
	unsigned char *key = NULL;
	int key_len = 0;
	int i;

	HMAC_CTX_init(&hmac_ctx);

	aws_sigv4_derive_signing_key(aws_secret_access_key,
		yyyy_mm_dd, region, service, &key, &key_len);

	HMAC_Init_ex(&hmac_ctx, key, key_len, EVP_sha256(), NULL);
	HMAC_Update(&hmac_ctx, message, message_len);
	HMAC_Final(&hmac_ctx, md, &md_len);
	free(key);
	signature = malloc(md_len * 2 + 1);

	if (signature == NULL) {
		Warnx("aws_sigv4_create_signature: failed to allocate sig");
		HMAC_CTX_cleanup(&hmac_ctx);
		return NULL;
	}

	for (i = 0; i < md_len; i++) {
		sprintf(signature + i * 2, "%.2x", md[i]);
	}

	HMAC_CTX_cleanup(&hmac_ctx);

	return signature;
}

