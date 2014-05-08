/*
 * Copyright (c) 2013-2014 Devicescape Software, Inc.
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
#include "aws.h"
#include <yajl/yajl_parse.h>
#include <stdlib.h>

#define AWS_IAM_CODE						"Code"
#define AWS_IAM_LAST_UPDATED			"LastUpdated"
#define AWS_IAM_TYPE						"Type"
#define AWS_IAM_ACCESS_KEY_ID			"AccessKeyId"
#define AWS_IAM_SECRET_ACCESS_KEY	"SecretAccessKey"
#define AWS_IAM_TOKEN					"Token"
#define AWS_IAM_EXPIRATION				"Expiration"

#define VALCMP(s, val, len) (len == strlen(s) && \
												strncmp(val, s, len) == 0)

enum {
	PARSER_STATE_NONE,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_CODE_KEY,
	PARSER_STATE_LAST_UPDATED_KEY,
	PARSER_STATE_TYPE_KEY,
	PARSER_STATE_ACCESS_KEY_ID_KEY,
	PARSER_STATE_SECRET_ACCESS_KEY_KEY,
	PARSER_STATE_TOKEN_KEY,
	PARSER_STATE_EXPIRATION_KEY,
};

static const char *parser_state_strings[] = {
	"none",
	"root map",
	"code key",
	"last updated key",
	"type key",
	"access key id key",
	"secret access key key",
	"token key",
	"expiration key",
};

static const char *parser_state_string(int state) {
	if (state < 0 || state > sizeof(parser_state_strings) / sizeof(parser_state_strings[0])) {
		return "invalid state";
	} else {
		return parser_state_strings[state];
	}
}

struct aws_iam_parse_credentials_ctx {
	char *id;
	char *key;
	char *token;
	time_t expiration;
	int parser_state;
};

static int aws_iam_parse_credentials_number(void * ctx, const char *val, unsigned int len)
{
#ifdef DEBUG_PARSER
	struct aws_iam_parse_credentials_ctx *_ctx = (struct aws_iam_parse_credentials_ctx *) ctx;
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("aws_iam_parse_credentials_number, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	Warnx("aws_iam_parse_credentials_number: unexpected json.");

#ifdef DEBUG_PARSER
	Debug("aws_iam_parse_credentials_number exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int aws_iam_parse_credentials_string(void *ctx, const unsigned char *val,
				 unsigned int len)
{
	struct aws_iam_parse_credentials_ctx *_ctx = (struct aws_iam_parse_credentials_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("aws_iam_parse_credentials_string, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ACCESS_KEY_ID_KEY: {
			_ctx->id = strndup(val, len);
			if (_ctx->id == NULL) {
				Warnx("aws_iam_parse_credentials_string: failed to allocate access key value");
				return 0;
			}
			break;
		}
		case PARSER_STATE_SECRET_ACCESS_KEY_KEY: {
			_ctx->key = strndup(val, len);
			if (_ctx->key == NULL) {
				Warnx("aws_iam_parse_credentials_string: failed to allocate secret key value");
				return 0;
			}
			break;
		}
		case PARSER_STATE_EXPIRATION_KEY: {
			time_t t;
			char *str = strndup(val, len);

			if (str != NULL) {
				t = aws_parse_iso8601_date(str);
				if (t != -1) {
					_ctx->expiration = t;
				} else {
					Warnx("aws_iam_parse_credentials_string: failed to parse date: %s", str);
				 	/* Mark credentials as already expired; we need to try again. */
					_ctx->expiration = 0;
				}
			} else {
				Warnx("aws_iam_parse_credentials_string: failed to allocate date");
				/* Mark credentials as already expired; we need to try again. */
				_ctx->expiration = 0;
			}
			free(str);
	
			break;
		}
		case PARSER_STATE_TOKEN_KEY: {
			_ctx->token = strndup(val, len);
			if (_ctx->token == NULL) {
				Warnx("aws_iam_parse_credentials_string: failed to allocate token value");
				return 0;
			}
			break;
		}
		case PARSER_STATE_CODE_KEY:
		case PARSER_STATE_LAST_UPDATED_KEY:
		case PARSER_STATE_TYPE_KEY: {
			/* ignored. */
			break;
		}
		default: {
			Warnx("aws_iam_parse_credentials_string - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

	_ctx->parser_state = PARSER_STATE_ROOT_MAP;

#ifdef DEBUG_PARSER
	Debug("aws_iam_parse_credentials_string exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int aws_iam_parse_credentials_start_map(void *ctx)
{
	struct aws_iam_parse_credentials_ctx *_ctx = (struct aws_iam_parse_credentials_ctx *) ctx;
#ifdef DEBUG_PARSER
	Debug("aws_iam_parse_credentials_start_map, enter state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_NONE: {
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		default: {
			Warnx("aws_iam_parse_credentials_start_map - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 1;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("aws_iam_parse_credentials_start_map exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}

static int aws_iam_parse_credentials_map_key(void *ctx, const unsigned char *val,
				  unsigned int len)
{
	struct aws_iam_parse_credentials_ctx *_ctx = (struct aws_iam_parse_credentials_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("aws_iam_parse_credentials_map_key, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ROOT_MAP: {
			if (VALCMP(AWS_IAM_CODE, val, len)) {
				_ctx->parser_state = PARSER_STATE_CODE_KEY;
			} else if (VALCMP(AWS_IAM_LAST_UPDATED, val, len)) {
				_ctx->parser_state = PARSER_STATE_LAST_UPDATED_KEY;
			} else if (VALCMP(AWS_IAM_TYPE, val, len)) {
				_ctx->parser_state = PARSER_STATE_TYPE_KEY;
			} else if (VALCMP(AWS_IAM_ACCESS_KEY_ID, val, len)) {
				_ctx->parser_state = PARSER_STATE_ACCESS_KEY_ID_KEY;
			} else if (VALCMP(AWS_IAM_SECRET_ACCESS_KEY, val, len)) {
				_ctx->parser_state = PARSER_STATE_SECRET_ACCESS_KEY_KEY;
			} else if (VALCMP(AWS_IAM_TOKEN, val, len)) {
				_ctx->parser_state = PARSER_STATE_TOKEN_KEY;
			} else if (VALCMP(AWS_IAM_EXPIRATION, val, len)) {
				_ctx->parser_state = PARSER_STATE_EXPIRATION_KEY;
			} else {
				Warnx("aws_iam_parse_credentials_map_key: Unknown key.");
				return 1;
			}
			break;
		}
		default: {
			Warnx("aws_iam_parse_credentials_map_key - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 1;
			break;
		}
	}


#ifdef DEBUG_PARSER
	Debug("aws_iam_parse_credentials_map_key exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}

static int aws_iam_parse_credentials_end_map(void *ctx)
{
	struct aws_iam_parse_credentials_ctx *_ctx = (struct aws_iam_parse_credentials_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("aws_iam_parse_credentials_end_map enter state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ROOT_MAP:{
				_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
		default: {
			Warnx("aws_iam_parse_credentials_map_end - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 1;
			break;
		}
	}


#ifdef DEBUG_PARSER
	Debug("aws_iam_parse_credentials_end_map exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}

static int aws_iam_parse_credentials_start_array(void *ctx)
{
#ifdef DEBUG_PARSER
	struct aws_iam_parse_credentials_ctx *_ctx = (struct aws_iam_parse_credentials_ctx *)ctx;
	Debug("aws_iam_parse_credentials_start_array enter state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */
	Warnx("aws_iam_parse_credentials_start_array: unexpected json.");
	return 1;
}

static int aws_iam_parse_credentials_end_array(void *ctx)
{
#ifdef DEBUG_PARSER
	struct aws_iam_parse_credentials_ctx *_ctx = (struct aws_iam_parse_credentials_ctx *)ctx;
	Debug("aws_iam_parse_credentials_end_array enter state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */
	Warnx("aws_iam_parse_credentials_start_array: unexpected json.");
	return 1;
}

static yajl_callbacks aws_iam_parse_credentials_callbacks = {  
	.yajl_number = aws_iam_parse_credentials_number,
	.yajl_string = aws_iam_parse_credentials_string,
	.yajl_start_map = aws_iam_parse_credentials_start_map,
	.yajl_map_key = aws_iam_parse_credentials_map_key,
	.yajl_end_map = aws_iam_parse_credentials_end_map,
	.yajl_start_array = aws_iam_parse_credentials_start_array,
	.yajl_end_array = aws_iam_parse_credentials_end_array,
};

static struct aws_session_token *aws_iam_parse_credentials(const char *response, int response_len) {
	yajl_handle hand;
	yajl_status stat;
	struct aws_iam_parse_credentials_ctx _ctx = { 0 };
	struct aws_session_token *token;

#if YAJL_MAJOR == 2
	hand = yajl_alloc(&aws_iam_parse_credentials_callbacks, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_complete_parse(hand);
#else
	hand = yajl_alloc(&aws_iam_parse_credentials_callbacks, NULL, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_parse_complete(hand);
#endif

	if (stat != yajl_status_ok) {
		unsigned char * str = yajl_get_error(hand, 1, response, response_len);  
		Warnx("aws_iam_parse_credentials: json parse failed, '%s'", (const char *) str);  
		yajl_free_error(hand, str);  
		yajl_free(hand);
		return NULL;
	}

	yajl_free(hand);

	token = calloc(sizeof(*token), 1);

	if (token == NULL) {
		Warnx("aws_iam_parse_credentials: Failed to allocate token.");
		return NULL;
	}

	token->session_token = _ctx.token;
	token->access_key_id = _ctx.id;
	token->secret_access_key = _ctx.key;
	token->expiration = _ctx.expiration;
	return token;
}
	
#define DEFAULT_TOKEN_URL "http://169.254.169.254/latest/meta-data/iam/security-credentials/"

struct aws_session_token *aws_iam_load_default_token(struct aws_handle *aws) {
	const char *response;
	int response_len;
	struct aws_session_token *token;
	char creds_url[128];
	int n;

	if (http_fetch_url(aws->http, DEFAULT_TOKEN_URL,
		HTTP_CLOSE, NULL) != HTTP_OK) {
		Warnx("aws_iam_load_default_token: Failed to get role.");
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_iam_load_default_token: Failed to get response.");
		return NULL; 
	}

	n = snprintf(creds_url, sizeof(creds_url), DEFAULT_TOKEN_URL "%s", response);

	if (n == -1 || n >= sizeof(creds_url)) {
		Warnx("aws_iam_load_default_token: buffer too small.");
		return NULL;
	}

	if (http_fetch_url(aws->http, creds_url, HTTP_CLOSE, NULL) != HTTP_OK) {
		Warnx("aws_iam_load_default_token: Failed to get security credentials.");
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if ((token = aws_iam_parse_credentials(response, response_len)) == NULL) {
		Warnx("aws_iam_load_default_token: Failed to parse response.");
		return NULL; 
	}
	return token;
}

