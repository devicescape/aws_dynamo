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

#ifndef _AWS_H_
#define _AWS_H_

#define CONF OPENSSL_CONF
#include <openssl/hmac.h>
#undef CONF

#define AWS_SESSION_REFRESH_TIME (60 * 5) /* seconds */

struct aws_session_token {
	char *session_token;
	char *secret_access_key;
	time_t expiration;	/* UTC expiration */
	char *access_key_id;
};

struct aws_handle {
	void *http;
	struct aws_session_token *token;
	char *aws_id;
	char *aws_key;

	HMAC_CTX hmac_ctx;

	/* Last DynamoDB error info. */
	char dynamo_message[512];
	int dynamo_errno;

	int dynamo_max_retries;
	int dynamo_https;
};


struct aws_handle *aws_init(const char *aws_id, const char *aws_key);

void aws_deinit(struct aws_handle *aws);

char *aws_create_signature(struct aws_handle *aws, const unsigned char *message, int message_len, const void *key, int key_len);

time_t aws_parse_iso8601_date(char *str);

void aws_free_session_token(struct aws_session_token *token);

int aws_get_security_credentials(struct aws_handle *aws, char **id, char **key);

#endif /* _AWS_H_ */
