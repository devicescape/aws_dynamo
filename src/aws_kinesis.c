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

#include <openssl/sha.h>

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include <yajl/yajl_parse.h>

#include "http.h"
#include "aws.h"
#include "aws_kinesis.h"
#include "aws_iam.h"
#include "aws_sigv4.h"
#include "aws_dynamo.h"
#include "aws_dynamo_query.h"

struct aws_errors aws_kinesis_errors[] = {
    { .code=AWS_KINESIS_CODE_UNKNOWN,                       .http_code=-1,  .error="Unknown",                     .reason="Unknown"},
    { .code=AWS_KINESIS_CODE_NONE,                          .http_code=200, .error="None",                        .reason="Success."},
    { .code=AWS_KINESIS_CODE_INCOMPLETE_SIGNATURE,          .http_code=400, .error="IncompleteSignature",         .reason="The request signature does not conform to AWS standards."},
    { .code=AWS_KINESIS_CODE_INTERNAL_FAILURE,              .http_code=500, .error="InternalFailure",             .reason="The request processing has failed because of an unknown error, exception or failure."},
    { .code=AWS_KINESIS_CODE_INVALID_ACTION,                .http_code=400, .error="InvalidAction",               .reason="The action or operation requested is invalid. Verify that the action is typed correctly."},
    { .code=AWS_KINESIS_CODE_INVALID_CLIENTTOKENID,         .http_code=403, .error="InvalidClientTokenId",        .reason="The X.509 certificate or AWS access key ID provided does not exist in our records."},
    { .code=AWS_KINESIS_CODE_INVALID_PARAMETER_COMBINATION, .http_code=400, .error="InvalidParameterCombination", .reason="Parameters that must not be used together were used together."},
    { .code=AWS_KINESIS_CODE_INVALID_PARAMETER_VALUE,       .http_code=400, .error="InvalidParameterValue",       .reason="An invalid or out-of-range value was supplied for the input parameter."},
    { .code=AWS_KINESIS_CODE_INVALID_QUERY_PARAMETER,       .http_code=400, .error="InvalidQueryParameter",       .reason="The AWS query string is malformed or does not adhere to AWS standards."},
    { .code=AWS_KINESIS_CODE_MALFORMED_QUERY_STRING,        .http_code=404, .error="MalformedQueryString",        .reason="The query string contains a syntax error."},
    { .code=AWS_KINESIS_CODE_MISSING_ACTION,                .http_code=400, .error="MissingAction",               .reason="The request is missing an action or a required parameter."},
    { .code=AWS_KINESIS_CODE_MISSING_AUTHENTICATION_TOKEN,  .http_code=403, .error="MissingAuthenticationToken",  .reason="The request must contain either a valid (registered) AWS access key ID or X.509 certificate."},
    { .code=AWS_KINESIS_CODE_MISSING_PARAMETER,             .http_code=400, .error="MissingParameter",            .reason="A required parameter for the specified action is not supplied."},
    { .code=AWS_KINESIS_CODE_OPT_IN_REQUIRED,               .http_code=403, .error="OptInRequired",               .reason="The AWS access key ID needs a subscription for the service."},
    { .code=AWS_KINESIS_CODE_REQUEST_EXPIRED,               .http_code=400, .error="RequestExpired",              .reason="The request reached the service more than 15 minutes after the date stamp on the request or more than 15 minutes after the request expiration date (such as for pre-signed URLs), or the date stamp on the request is more than 15 minutes in the future."},
    { .code=AWS_KINESIS_CODE_SERVICE_UNAVAILABLE,           .http_code=503, .error="ServiceUnavailable",          .reason="The request has failed due to a temporary failure of the server."},
    { .code=AWS_KINESIS_CODE_THROTTLING,                    .http_code=400, .error="Throttling",                  .reason="The request was denied due to request throttling."},
    { .code=AWS_KINESIS_CODE_VALIDATION_ERROR,              .http_code=400, .error="ValidationError",             .reason="The input fails to satisfy the constraints specified by an AWS service."},
    { .code=AWS_KINESIS_CODE_SERIALIZATION_ERROR,           .http_code=400, .error="SerializationException",      .reason="Likely that invalid JSON has been sent."},
};

enum {
	ERROR_RESPONSE_PARSER_STATE_NONE,
	ERROR_RESPONSE_PARSER_STATE_ROOT_MAP,
	ERROR_RESPONSE_PARSER_STATE_TYPE_KEY,
	ERROR_RESPONSE_PARSER_STATE_MESSAGE_KEY,
};

struct error_response_parser_ctx {
	int code;
	char *message;
	int parser_state;
};


static int kinesis_error_response_parser_string(void *ctx, const unsigned char *val, unsigned int len)
{
	struct error_response_parser_ctx *_ctx = (struct error_response_parser_ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("error_response_parser_string, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case ERROR_RESPONSE_PARSER_STATE_TYPE_KEY: {
        size_t i;
        size_t count=sizeof(aws_kinesis_errors)/sizeof(aws_kinesis_errors[0]);
        int found = -1;

        for (i=0; i<count; i++) {
            struct aws_errors *aws_error = &aws_kinesis_errors[i];
        	if (AWS_DYNAMO_VALCMP(aws_error->error, val, len)) {
                _ctx->code = aws_error->code;
                found = i;
                break;
            }
		}
        if (found==-1) {
			char code[len + 1];
			snprintf(code, len + 1, "%s", val);
			Warnx("kinesis_error_response_parser_string - unknown code %s", code);
		}
		_ctx->parser_state = ERROR_RESPONSE_PARSER_STATE_ROOT_MAP;
		break;
	}
	case ERROR_RESPONSE_PARSER_STATE_MESSAGE_KEY: {
		free(_ctx->message);
		_ctx->message = strndup(val, len);
		_ctx->parser_state = ERROR_RESPONSE_PARSER_STATE_ROOT_MAP;
		break;
	}
	default:{
			Warnx("kinesis_error_response_parser_string - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("kinesis_error_response_parser_string exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int kinesis_error_response_parser_start_map(void *ctx)
{
	struct error_response_parser_ctx *_ctx = (struct error_response_parser_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("kinesis_error_response_parser_start_map, enter state %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case ERROR_RESPONSE_PARSER_STATE_NONE: {
		_ctx->parser_state = ERROR_RESPONSE_PARSER_STATE_ROOT_MAP;
		break;
	}
	default:{
			Warnx("kinesis_error_response_parser_start_map - unexpected state: %d",
			     _ctx->parser_state);
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("kinesis_error_response_parser_start_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int kinesis_error_response_parser_map_key(void *ctx, const unsigned char *val,
				  unsigned int len)
{
	struct error_response_parser_ctx *_ctx = (struct error_response_parser_ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	Debug("kinesis_error_response_parser_map_key, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case ERROR_RESPONSE_PARSER_STATE_ROOT_MAP: {
			if (AWS_DYNAMO_VALCMP("__type", val, len)) {
				_ctx->parser_state = ERROR_RESPONSE_PARSER_STATE_TYPE_KEY;
			} else if (AWS_DYNAMO_VALCASECMP("message", val, len)) {
				_ctx->parser_state = ERROR_RESPONSE_PARSER_STATE_MESSAGE_KEY;
			}
		break;
	}
	default:{
			Warnx("kinesis_error_response_parser_map_key - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("kinesis_error_response_parser_map_key exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int kinesis_error_response_parser_end_map(void *ctx)
{
	struct error_response_parser_ctx *_ctx = (struct error_response_parser_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("kinesis_error_response_parser_end_map enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case ERROR_RESPONSE_PARSER_STATE_ROOT_MAP:{
			_ctx->parser_state = ERROR_RESPONSE_PARSER_STATE_NONE;
			break;
		}
	default:{
			Warnx("kinesis_error_response_parser_end_map - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("error_response_parser_end_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static yajl_callbacks kinesis_error_response_parser_callbacks = {
	.yajl_string    = kinesis_error_response_parser_string,
	.yajl_start_map = kinesis_error_response_parser_start_map,
	.yajl_map_key   = kinesis_error_response_parser_map_key,
	.yajl_end_map   = kinesis_error_response_parser_end_map,
};

/* returns 1 if the request should be retried, 0 if the request shouldn't be
	retried, -1 on error. */
static int aws_kinesis_parse_error_response(const unsigned char *response, int response_len, char **message, int *code)
{
	yajl_handle hand;
	yajl_status stat;
	struct error_response_parser_ctx _ctx = {
		.code = AWS_KINESIS_CODE_UNKNOWN,
	};
	int rv;

#if YAJL_MAJOR == 2
	hand = yajl_alloc(&kinesis_error_response_parser_callbacks, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_complete_parse(hand);
#else
	hand = yajl_alloc(&kinesis_error_response_parser_callbacks, NULL, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_parse_complete(hand);
#endif

	if (stat != yajl_status_ok) {
		unsigned char *str =
		    yajl_get_error(hand, 1, response, response_len);
		Warnx("aws_kinesis_parse_error_response: json parse failed, '%s'", (const char *)str);
		yajl_free_error(hand, str);
		yajl_free(hand);
		free(_ctx.message);
		*code = AWS_KINESIS_CODE_UNKNOWN;
		return -1;
	}

	if (_ctx.message != NULL) {
		free(*message);
		*message = strdup(_ctx.message);
		free(_ctx.message);
		*code = _ctx.code;
	}

	switch(_ctx.code) {
    case AWS_KINESIS_CODE_MISSING_ACTION:
    case AWS_KINESIS_CODE_MISSING_AUTHENTICATION_TOKEN:
    case AWS_KINESIS_CODE_MISSING_PARAMETER:
    case AWS_KINESIS_CODE_OPT_IN_REQUIRED:
    case AWS_KINESIS_CODE_REQUEST_EXPIRED:
    case AWS_KINESIS_CODE_INVALID_PARAMETER_COMBINATION:
    case AWS_KINESIS_CODE_INVALID_PARAMETER_VALUE:
    case AWS_KINESIS_CODE_INVALID_QUERY_PARAMETER:
    case AWS_KINESIS_CODE_MALFORMED_QUERY_STRING:
    case AWS_KINESIS_CODE_INCOMPLETE_SIGNATURE:
    case AWS_KINESIS_CODE_INVALID_CLIENTTOKENID:
    case AWS_KINESIS_CODE_INVALID_ACTION:
    case AWS_KINESIS_CODE_VALIDATION_ERROR:
		/* the request should not be retried. */
		rv = 0;
		break;

    case AWS_KINESIS_CODE_THROTTLING:
    case AWS_KINESIS_CODE_INTERNAL_FAILURE:
    case AWS_KINESIS_CODE_SERVICE_UNAVAILABLE:
		/* the request should be retried. */
		rv = 1;
		break;

	case AWS_KINESIS_CODE_UNKNOWN:
	default:
		rv = -1;
		break;
	}

	yajl_free(hand);
	return rv;
}

int aws_kinesis_request(struct aws_handle *aws, const char *target, const char *body) {
	int http_response_code;
	int kinesis_response_code = AWS_KINESIS_CODE_UNKNOWN;
	int rv = -1;
	int attempt = 0;
	char *message = NULL;
    /* FIXME: Change aws handle variables to be generic, not just dynamodb? */
	do {

		if (aws_post(aws, "kinesis", target, body) == -1) {
			return -1;
		}
	
		http_response_code = http_get_response_code(aws->http);
#ifdef DEBUG_AWS_KINESIS
        Debug("%s:%d http_response_code:%d", __FILE__, __LINE__, http_response_code);
#endif
		if (http_response_code == 200) {
			rv = 0;
			break;
		} else {
			const char *response;
			int response_len;
			int retry;
			useconds_t backoff;

			response = http_get_data(aws->http, &response_len);

			if (response == NULL) {
				Warnx("aws_kinesis_request: Failed to get error response.");
				break;
			}

			retry = aws_kinesis_parse_error_response(response, response_len, &message, &kinesis_response_code);
			if (retry == 0) {
				/* Don't retry. */
				break;
			} else if (retry != 1) {
				Warnx("aws_kinesis_request: Error evaluating error body. target='%s' body='%s' response='%s'",
						target, body, response);
				break;
			}

			backoff = (1 << attempt) * (rand() % 50000 + 25000);
			Warnx("aws_kinesis_request: '%s' will retry after %d ms wait, attempt %d: %s %s",
				message ? message : "unknown error", backoff / 1000, attempt, target, body);
			usleep(backoff);
			attempt++;
		}

	} while (attempt < aws->dynamo_max_retries);

	if (attempt >= aws->dynamo_max_retries) {
			Warnx("aws_kinesis_request: max retry limit hit, giving up: %s %s", target, body);
	}

	if (message != NULL) {
		snprintf(aws->dynamo_message, sizeof(aws->dynamo_message), "%s",
			message);
		free(message);
		aws->dynamo_errno = kinesis_response_code;
	} else {
		aws->dynamo_message[0] = '\0';
		aws->dynamo_errno = AWS_KINESIS_CODE_NONE;
	}

	return rv;
}
