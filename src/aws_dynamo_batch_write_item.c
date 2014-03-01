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

#define _GNU_SOURCE

#include "aws_dynamo_utils.h"

#include <stdlib.h>

#include <yajl/yajl_parse.h>

#include "http.h"
#include "aws_dynamo.h"

#define DEBUG_PARSER 1

enum {
	PARSER_STATE_NONE = 0,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_RESPONSES_KEY,
	PARSER_STATE_RESPONSES_MAP,
	PARSER_STATE_TABLE_NAME_KEY,
	PARSER_STATE_TABLE_NAME_MAP,
	PARSER_STATE_CONSUMED_CAPACITY_KEY,
	PARSER_STATE_UNPROCESSED_ITEMS_KEY,
};

static const char *parser_state_strings[] = {
	"none",
	"root map",
	"responses key",
	"responses map",
	"table name key",
	"table name map",
	"consumed capacity key",
	"unprocessed items key",
};

static const char *parser_state_string(int state)
{
	if (state < 0 ||
	    state >
	    sizeof(parser_state_strings) / sizeof(parser_state_strings[0])) {
		return "invalid state";
	} else {
		return parser_state_strings[state];
	}
}

struct ctx {
	struct aws_dynamo_batch_write_item_response *r;
	int table_num;
	int parser_state;
};

static int handle_number(void *ctx, const char *val, unsigned int len)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("handle_number, val = %s, enter state '%s'", buf,
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_CONSUMED_CAPACITY_KEY:{
			if (aws_dynamo_json_get_double(val, len,
						       &(_ctx->
							 r->responses[_ctx->
								      r->num_responses].consumed_capacity_units))
			    == -1) {
				return 0;
			}
		_ctx->parser_state = PARSER_STATE_TABLE_NAME_MAP;
		}
		break;
	default:{
			Warnx("handle_number - unexpected state '%s'",
			      parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_number exit state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_string(void *ctx, const unsigned char *val, unsigned int len)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("handle_string, val = %s, enter state '%s'", buf,
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	default:{
			Warnx("handle_string - unexpected state '%s'",
			      parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_string exit state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_start_map(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;

#ifdef DEBUG_PARSER
	Debug("handle_start_map, enter state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_NONE:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_RESPONSES_KEY:{
			_ctx->parser_state = PARSER_STATE_RESPONSES_MAP;
			break;
		}
	case PARSER_STATE_TABLE_NAME_KEY:{
			_ctx->parser_state = PARSER_STATE_TABLE_NAME_MAP;
			break;
		}
	default:{
			Warnx("handle_start_map - unexpected state: %s",
			      parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_start_map exit state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */
	return 1;
}

static int handle_map_key(void *ctx, const unsigned char *val, unsigned int len)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("handle_map_key, val = %s, enter state '%s'", buf,
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			if (AWS_DYNAMO_VALCMP
			    (AWS_DYNAMO_JSON_RESPONSES, val, len)) {
				_ctx->parser_state = PARSER_STATE_RESPONSES_KEY;
			} else
			    if (AWS_DYNAMO_VALCMP
				(AWS_DYNAMO_JSON_UNPROCESSED_ITEMS, val, len)) {
				_ctx->parser_state =
				    PARSER_STATE_UNPROCESSED_ITEMS_KEY;
			} else {
				Warnx("handle_map_key: Unknown key.");
				return 0;
			}
			break;
		}
	case PARSER_STATE_RESPONSES_MAP:{
			char *table_name = strndup(val, len);
			struct aws_dynamo_batch_write_item_consumed_capacity *new_responses;

			if (table_name == NULL) {
				Warnx("handle_map_key: alloc failed.");
				return 0;
			}

			new_responses = realloc(_ctx->r->responses,
				_ctx->r->num_responses + 1 * sizeof(_ctx->r->responses[0]));
			if (new_responses == NULL) {
				Warnx("handle_map_key: realloc failed.");
				free(table_name);
				return 0;
			}
			_ctx->r->responses = new_responses;
			_ctx->r->responses[_ctx->r->num_responses].table_name =
			    table_name;
			_ctx->r->num_responses += 1;
			_ctx->parser_state = PARSER_STATE_TABLE_NAME_KEY;
			break;
		}
	case PARSER_STATE_TABLE_NAME_MAP:{
			_ctx->parser_state = PARSER_STATE_CONSUMED_CAPACITY_KEY;
			break;
		}
	default:{
			Warnx("handle_map_key - unexpected state: %s",
			      parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_map_key exit state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */
	return 1;
}

static int handle_end_map(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_end_map enter state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */
	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
	case PARSER_STATE_TABLE_NAME_MAP:{
			_ctx->parser_state = PARSER_STATE_RESPONSES_MAP;
			break;
		}
	default:{
			Warnx("handle_end_map - unexpected state: %s",
			      parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("handle_end_map exit state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_start_array(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_start_array enter state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	default:{
			Warnx
			    ("handle_start_array - unexpected state '%s'",
			     parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_start_array exit state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_end_array(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_end_array enter state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	default:{
			Warnx
			    ("handle_end_array - unexpected state '%s'",
			     parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_end_array exit state '%s'",
	      parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static yajl_callbacks handle_callbacks = {
	.yajl_number = handle_number,
	.yajl_string = handle_string,
	.yajl_start_map = handle_start_map,
	.yajl_map_key = handle_map_key,
	.yajl_end_map = handle_end_map,
	.yajl_start_array = handle_start_array,
	.yajl_end_array = handle_end_array,
};

struct aws_dynamo_batch_write_item_response
*aws_dynamo_parse_batch_write_item_response(const char *response,
					    int response_len)
{
	yajl_handle hand;
	yajl_status stat;
	struct ctx _ctx = { 0 };

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx
		    ("aws_dynamo_parse_batch_write_item_response: alloc failed.");
		return NULL;
	}

	hand = yajl_alloc(&handle_callbacks, NULL, NULL, &_ctx);

	yajl_parse(hand, response, response_len);

	stat = yajl_parse_complete(hand);

	if (stat != yajl_status_ok) {
		unsigned char *str =
		    yajl_get_error(hand, 1, response, response_len);
		Warnx
		    ("aws_dynamo_parse_batch_write_item_response: json parse failed, '%s'",
		     (const char *)str);
		yajl_free_error(hand, str);
		yajl_free(hand);
		aws_dynamo_free_batch_write_item_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_dynamo_batch_write_item_response
*aws_dynamo_batch_write_item(struct aws_handle *aws, const char *request)
{
	const char *response;
	int response_len;
	struct aws_dynamo_batch_write_item_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_BATCH_WRITE_ITEM, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_batch_write_item: Failed to get response.");
		return NULL;
	}

	if ((r =
	     aws_dynamo_parse_batch_write_item_response(response, response_len))
	    == NULL) {
		Warnx("aws_dynamo_batch_write_item: Failed to parse response.");
		return NULL;
	}

	return r;
}

void aws_dynamo_dump_batch_write_item_response(struct
					       aws_dynamo_batch_write_item_response
					       *r)
{
#ifdef DEBUG_PARSER
	int i;
	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	if (r->unprocessed_items) {
		Debug("unprocessed items: %s", r->unprocessed_items);
	}

	Debug("%d responses:", r->num_responses);
	for (i = 0; i < r->num_responses; i++) {
		Debug("%s: %f capacity units",
		      r->responses[i].table_name,
		      r->responses[i].consumed_capacity_units);
	}
#endif
}

void aws_dynamo_free_batch_write_item_response(struct aws_dynamo_batch_write_item_response
					       *r)
{
	int i;

	if (r == NULL) {
		return;
	}

	free(r->unprocessed_items);

	for (i = 0; i < r->num_responses; i++) {
		free(r->responses[i].table_name);
	}
	free(r->responses);

	free(r);
}
