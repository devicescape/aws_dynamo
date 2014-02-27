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
#include "http.h"
#include "aws_dynamo.h"
#include "aws_iam.h"
#include "aws.h"

#include <openssl/engine.h>
#include <openssl/sha.h>
#include <openssl/buffer.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/evp.h>

#include <stdlib.h>
#include <stdio.h>

#define AWS_DYNAMO_DEFAULT_MAX_RETRIES	10
#define AWS_DYNAMO_DEFAULT_HTTPS			1

struct aws_handle *aws_init(const char *aws_id, const char *aws_key) {
	struct aws_handle *aws = NULL;

	/* Initialize openssl. */
	ENGINE_load_builtin_engines();
	ENGINE_register_all_complete();

	aws = calloc(sizeof(*aws), 1);

	if (aws == NULL) {
		Errx("aws_init: Failed to allocate aws structure.");
		goto error;
	}

	aws->http = http_init();

	if (aws->http == NULL) {
		Errx("aws_init: Failed to initialize http handle.");
		goto error;
	}

	if (aws_id == NULL || aws_key == NULL) {
		/* Try to get credentials from environment. */
		aws_id = getenv("AWS_ACCESS_KEY_ID");
		aws_key = getenv("AWS_SECRET_ACCESS_KEY");
	}

	if (aws_id == NULL || aws_key == NULL) {
		/* Get token from EC2 Role. */
		aws->token = aws_iam_load_default_token(aws);
		if (aws->token == NULL) {
			Errx("aws_init: Failed to get aws token.");
			goto error;
		}
	} else {
		aws->aws_id = strdup(aws_id);

		if (aws->aws_id == NULL) {
			Errx("aws_init: Failed to copy id.");
			goto error;
		}

		aws->aws_key = strdup(aws_key);

		if (aws->aws_key == NULL) {
			Errx("aws_init: Failed to copy id.");
			goto error;
		}
	}

	aws->dynamo_max_retries = AWS_DYNAMO_DEFAULT_MAX_RETRIES;
	aws->dynamo_https = AWS_DYNAMO_DEFAULT_HTTPS;
	aws->dynamo_host = NULL;
	aws->dynamo_port = 0;

	return aws;

error:

	aws_deinit(aws);

	return NULL;
}


void aws_deinit(struct aws_handle *aws) {

	if (aws != NULL) {

		if (aws->http != NULL) {
			http_deinit(aws->http);
		}
		free(aws->aws_id);
		free(aws->aws_key);
		free(aws->dynamo_host);
		aws_free_session_token(aws->token);

		free(aws);
	}
}

time_t aws_parse_iso8601_date(char *str) {
	struct tm t;
	time_t time;
	char *ptr;

	if ((ptr = strptime(str, "%Y-%m-%dT%H:%M:%S", &t)) == NULL) {
		Warnx("aws_parse_iso8601_date: Failed to parse date.");
		return -1;
	}

	/* Skip over decimal seconds. */
	if (*ptr == '.')
		ptr++;

	while (isdigit(*ptr)) {
		ptr++;
	}

	if (*ptr != 'Z') {
		Warnx("aws_parse_iso8601_date: Unexpected timezone.");
		return -1;
	}

	time = timegm(&t);

	if (time == -1) {
		Warnx("aws_parse_iso8601_date: timegm() failed.");
		return -1;
	}
	return time;
}

void aws_free_session_token(struct aws_session_token *token)
{
	if (token != NULL) {
		free(token->session_token);
		free(token->secret_access_key);
		free(token->access_key_id);
		free(token);
	}
}

