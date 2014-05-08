/*
 * Copyright (c) 2013-2014 Devicescape Software, Inc.
 * This file is part of aws_dynamo, a C library for AWS.
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

#include <stdlib.h>

#include <yajl/yajl_parse.h>

#include "http.h"
#include "aws_dynamo.h"
#include "aws_kinesis.h"
#include "aws_kinesis_put_record.h"

enum {
	PARSER_STATE_NONE = 0,
	PARSER_STATE_ROOT_MAP,
    PARSER_STATE_SEQUENCE_NUMBER_KEY,
    PARSER_STATE_SEQUENCE_NUMBER_VALUE,
    PARSER_STATE_SHARD_ID_KEY,
    PARSER_STATE_SHARD_ID_VALUE,
};

struct put_record_ctx {
	struct aws_kinesis_put_record_response *r;
	int parser_state;
};

static int put_record_string(void *ctx, const unsigned char *val, unsigned int len)
{
	struct put_record_ctx *_ctx = (struct put_record_ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
Debug("put_record_string, val = %s, enter state %d", buf, _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case PARSER_STATE_SEQUENCE_NUMBER_VALUE:{
            _ctx->r->sequence_number = strndup(val, len);
            _ctx->parser_state = PARSER_STATE_ROOT_MAP;
            break;
        }
	case PARSER_STATE_SHARD_ID_VALUE:{
            _ctx->r->shard_id = strndup(val, len);
            _ctx->parser_state = PARSER_STATE_ROOT_MAP;
            break;
        }
	default:{
            Warnx("put_record_string - unexpected state %d", _ctx->parser_state);
            return 0;
            break;
        }
    }
#ifdef DEBUG_PARSER
	Debug("put_record_string exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
} 

static int put_record_start_map(void *ctx)
{
	struct put_record_ctx *_ctx = (struct put_record_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("put_record_start_map, enter state %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
    switch (_ctx->parser_state) {
	case PARSER_STATE_NONE:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_SEQUENCE_NUMBER_KEY:{
		    _ctx->parser_state = PARSER_STATE_SEQUENCE_NUMBER_VALUE;
			break;
		}
	case PARSER_STATE_SHARD_ID_KEY:{
			_ctx->parser_state = PARSER_STATE_SHARD_ID_VALUE;
			break;
		}
	default:{
			Warnx("put_record_start_map - unexpected state: %d", _ctx->parser_state);
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("put_record_start_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int put_record_map_key(void *ctx, const unsigned char *val,
				  unsigned int len)
{
	struct put_record_ctx *_ctx = (struct put_record_ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("put_record_map_key, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_KINESIS_JSON_SEQUENCE_NUMBER, val, len)) {
				_ctx->parser_state = PARSER_STATE_SEQUENCE_NUMBER_VALUE;
			} else if (AWS_DYNAMO_VALCMP(AWS_KINESIS_JSON_SHARD_ID, val, len)) {
				_ctx->parser_state = PARSER_STATE_SHARD_ID_VALUE;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("put_record_map_key: Unknown root key '%s'.", key);
				return 0;
			}
			break;
		}
	default:{
			Warnx("put_record_map_key - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("put_record_map_key exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int put_record_end_map(void *ctx)
{
	struct put_record_ctx *_ctx = (struct put_record_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("put_record_end_map enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
	default:{
			Warnx("put_record_end_map - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("put_record_end_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static yajl_callbacks put_record_callbacks = {
	.yajl_string = put_record_string,
	.yajl_start_map = put_record_start_map,
	.yajl_map_key = put_record_map_key,
	.yajl_end_map = put_record_end_map,
};

struct aws_kinesis_put_record_response * aws_kinesis_parse_put_record_response(const char *response, int response_len)
{
	yajl_handle hand;
	yajl_status stat;
	struct put_record_ctx _ctx = { 0 };

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx("aws_kinesis_parse_put_record_response: response alloc failed.");
		return NULL;
	}

#if YAJL_MAJOR == 2
	hand = yajl_alloc(&put_record_callbacks, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_complete_parse(hand);
#else
	hand = yajl_alloc(&put_record_callbacks, NULL, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_parse_complete(hand);
#endif

	if (stat != yajl_status_ok) {
		unsigned char *str =
		    yajl_get_error(hand, 1, response, response_len);
		Warnx("aws_kinesis_parse_put_record_response: json parse failed, '%s'", (const char *)str);
		yajl_free_error(hand, str);
		yajl_free(hand);
		aws_kinesis_free_put_record_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_kinesis_put_record_response *aws_kinesis_put_record(struct aws_handle *aws, const char *request)
{
	const char *response;
	int response_len;
	struct aws_kinesis_put_record_response *r;

	if (aws_kinesis_request(aws, AWS_KINESIS_PUT_RECORD, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_kinesis_put_record: Failed to get response.");
		return NULL;
	}

	if ((r = aws_kinesis_parse_put_record_response(response, response_len)) == NULL) {
		Warnx("aws_kinesis_put_record: Failed to parse response.");
		return NULL;
	}

	return r;
}

void aws_kinesis_dump_put_record_response(struct aws_kinesis_put_record_response *r)
{
#ifdef DEBUG_PARSER
	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	Debug("sequence_number = %s", r->sequence_number);
	Debug("shard_id = %s", r->shard_id);

#endif
}

void aws_kinesis_free_put_record_response(struct aws_kinesis_put_record_response *r)
{
	if (r == NULL) {
		return;
	}
    free(r->sequence_number);
    free(r->shard_id);
	free(r);	
}

