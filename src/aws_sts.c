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

#include <expat.h>
#include "aws.h"
#include "aws_sts.h"
#include "http.h"

#define SESSION_TOKEN_ELEMENT_NONE		0
#define SESSION_TOKEN_ELEMENT_TOKEN		1
#define SESSION_TOKEN_ELEMENT_KEY		2
#define SESSION_TOKEN_ELEMENT_EXPIRE	3
#define SESSION_TOKEN_ELEMENT_ID			4

struct session_token_response_user_data {
	int depth;
	int element;
	struct aws_session_token *token;
};

#define SESSION_TOKEN_RESPONSE "GetSessionTokenResponse"
#define SESSION_TOKEN_RESULT "GetSessionTokenResult"
#define SESSION_TOKEN_CREDENTIALS "Credentials"
#define SESSION_TOKEN_TOKEN "SessionToken"
#define SESSION_TOKEN_KEY "SecretAccessKey"
#define SESSION_TOKEN_EXPIRE "Expiration"
#define SESSION_TOKEN_ID "AccessKeyId"

static char *aws_http_create_signature(struct aws_handle *aws, char *http_verb, char *host, char *uri,
	const char *query_string, const void *key, int key_len)
{
	char *message;
	char *signature;

	if (asprintf(&message, "%s\n%s\n%s\n%s", http_verb, host, uri, query_string) == -1) {
		Warnx("aws_http_create_signature: Failed to create message.");
		return NULL;
	}
	
	signature = aws_create_signature(aws, message, strlen(message), key, key_len);
	free(message);

	return signature;
}

static void session_token_start_element(void *user_data, const char *name, const char **atts)
{
	struct session_token_response_user_data *ud =
		(struct session_token_response_user_data *)user_data;

	ud->depth += 1;

	if (ud->depth == 4) {
		if (strcmp(name, SESSION_TOKEN_TOKEN) == 0) {
			ud->element = SESSION_TOKEN_ELEMENT_TOKEN;
		} else if (strcmp(name, SESSION_TOKEN_KEY) == 0) {
			ud->element = SESSION_TOKEN_ELEMENT_KEY;
		} else if (strcmp(name, SESSION_TOKEN_EXPIRE) == 0) {
			ud->element = SESSION_TOKEN_ELEMENT_EXPIRE;
		} else if (strcmp(name, SESSION_TOKEN_ID) == 0) {
			ud->element = SESSION_TOKEN_ELEMENT_ID;
		} else {
			ud->element = SESSION_TOKEN_ELEMENT_NONE;
		}
	} else {
		ud->element = SESSION_TOKEN_ELEMENT_NONE;
	}
}

static void session_token_end_element(void *user_data, const char *name)
{
	struct session_token_response_user_data *ud =
		(struct session_token_response_user_data *)user_data;

	ud->depth -= 1;
	ud->element = SESSION_TOKEN_ELEMENT_NONE;
}

static void session_token_char_data(void *user_data, const XML_Char *s, int len)
{
	struct session_token_response_user_data *ud =
		(struct session_token_response_user_data *)user_data;

	switch (ud->element) {
		case SESSION_TOKEN_ELEMENT_TOKEN: {
			free(ud->token->session_token);
			ud->token->session_token = strndup(s, len);
			break;
		}
		case SESSION_TOKEN_ELEMENT_KEY: {
			free(ud->token->secret_access_key);
			ud->token->secret_access_key = strndup(s, len);
			break;
		}
		case SESSION_TOKEN_ELEMENT_EXPIRE: {
			time_t t;
			char *str;

			str = strndup(s, len);

			if (str == NULL) {
				Warnx("session_token_char_data: Failed to allocate session token expiration.");
				return;
			}

			t = aws_parse_iso8601_date(str);
			if (t != -1) {
				ud->token->expiration = t;
			}
			free(str);
			break;
		}
		case SESSION_TOKEN_ELEMENT_ID: {
			free(ud->token->access_key_id);
			ud->token->access_key_id = strndup(s, len);
			break;
		}
	}
}

static struct aws_session_token *get_session_token_from_xml(const char *xml, int xml_len) {
	struct session_token_response_user_data user_data = { 0 };
	struct aws_session_token *token = NULL;
	XML_Parser parser = NULL;

	parser = XML_ParserCreate(NULL);
	if (parser == NULL) {
		Warnx("get_session_token_from_xml: Failed to create parser.");
		goto cleanup;
	}

	user_data.token = token = calloc(sizeof(*token), 1);
	if (token == NULL) {
		Warnx("get_session_token_from_xml: Failed to allocate token.");
		goto cleanup;
	}

	XML_SetUserData(parser, &user_data);
	XML_SetElementHandler(parser, session_token_start_element, session_token_end_element);
	XML_SetCharacterDataHandler(parser, session_token_char_data);

	if (XML_Parse(parser, xml, xml_len, 1) == XML_STATUS_ERROR) {	
		Warnx("get_session_token_from_xml: Error parsing session token: %s",
			XML_ErrorString(XML_GetErrorCode(parser)));
		aws_free_session_token(token);
		token = NULL;
		goto cleanup;
	}

	if (token->session_token == NULL || token->secret_access_key == NULL ||
		token->expiration == 0 || token->access_key_id == NULL) {
		Warnx("get_session_token_from_xml: Token incomplete.");
		aws_free_session_token(token);
		token = NULL;
	}

cleanup:

	if (parser != NULL) {
		XML_ParserFree(parser);
	}

	return token;
}

static int aws_compar_parameters(const void *a, const void *b)
{
	struct http_parameter *pa = (struct http_parameter *)a;
	struct http_parameter *pb = (struct http_parameter *)b;

	return strcmp(pa->name, pb->name);
}

static char *aws_http_get_canonicalized_query_string(struct aws_handle *aws, struct http_parameter *parameters, int num_parameters)
{
	char *message;
	char *signature;
	char *query_string;
	int query_string_len = 0;
	char *ptr;
	int i;

	/* The parameters must be ordered by name alphabetically, lower case first. */
	qsort(parameters, num_parameters, sizeof(parameters[0]),
	      aws_compar_parameters);

	query_string_len += num_parameters * 2; /* room for '=' and '&' */
	for (i = 0; i < num_parameters; i++) {
		query_string_len += strlen(parameters[i].name);

		/* Assume every value character needs to be percent encoded.  This is an
		   over-estimate but it ensures that the buffer will be long enough. */
		query_string_len += strlen(parameters[i].value) * 3;

	}

	query_string_len++; /* \0 terminator */

	ptr = query_string = calloc(sizeof(char), query_string_len);

	if (ptr == NULL) {
		Err("aws_http_get_canonicalized_query_string(): Failed to allocate query string.");
		return NULL;
	}

	for (i = 0; i < num_parameters; i++) {
		int n;
		char *value;
		int remaining;

		remaining = query_string_len - (ptr - query_string) - 1;

		value = http_url_encode(aws->http, parameters[i].value);

		if (ptr > query_string) {
			n = snprintf(ptr, remaining, "&%s=%s",
				     parameters[i].name, value);
		} else {
			n = snprintf(ptr, remaining, "%s=%s",
				     parameters[i].name, value);
		}

		free(value);

		if (n == -1 || n >= remaining) {
			Err("aws_http_get_canonicalized_query_string(): query string buffer not large enough.");
			free(query_string);
			return NULL;
		}

		ptr += n;
	}

	return query_string;
}

struct aws_session_token *aws_sts_get_session_token(struct aws_handle *aws,
	const char *id, const char *key) {
	char timestamp[120];
	struct http_parameter parameters[] = {
		{.name = "Action",.value = "GetSessionToken"},
		{.name = "AWSAccessKeyId", .value = id},
		{.name = "DurationSeconds",.value = AWS_STS_SESSION_DURATION},
		{.name = "Timestamp", .value = timestamp},
		{.name = "Version",.value = "2011-06-15"},
		{.name = "SignatureMethod",.value = "HmacSHA256"},
		{.name = "SignatureVersion",.value = "2"},
	};
	char *signature;
	char *encoded_signature;
	char *query_string;
	char *url;
	struct tm tm;
	time_t t;
	const char *data;
	int data_len;
	void *http = aws->http;

	time(&t);
	
	gmtime_r(&t, &tm);
	snprintf(timestamp, sizeof(timestamp), "%i-%02i-%02iT%02i:%02i:%02iZ",
		tm.tm_year + 1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

	query_string = aws_http_get_canonicalized_query_string(aws, parameters,
		sizeof(parameters) / sizeof(parameters[0]));
			
	if (query_string == NULL) {
		Warnx("aws_sts_get_session_token: Failed to get query string.");
		return NULL;
	}

	signature = aws_http_create_signature(aws, "GET", AWS_STS_HOST, "/", query_string,
			key, strlen(key));

	if (signature == NULL) {
		free(query_string);
		Warnx("aws_sts_get_session_token: Failed to get signature.");
		return NULL;
	}

	encoded_signature = http_url_encode(aws->http, signature);
	free(signature);
	
	if (asprintf(&url, "https://" AWS_STS_HOST "/?%s&Signature=%s", query_string,
		encoded_signature) == -1) {
		Warnx("aws_sts_get_session_token: asprintf failed.");
		url = NULL;
	}

	free(query_string);
	free(encoded_signature);

	if (url == NULL) {
		Warnx("aws_sts_get_session_token: Failed to allocate url.");
		return NULL;
	}

	if (_http_fetch_url(http, url, 1, NULL) != HTTP_OK) {
		Warnx("aws_sts_get_session_token: http fetch failed.");
		free(url);
		return NULL;
	}
	free(url);

	data = http_get_data(http, &data_len);

	if (data == NULL) {
		Warnx("aws_sts_get_session_token: Failed to get data.");
		return NULL;
	}

	return get_session_token_from_xml(data, data_len);
}

