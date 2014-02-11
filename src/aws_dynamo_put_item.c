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

#include <stdlib.h>

#include <yajl/yajl_parse.h>

#include "http.h"
#include "aws_dynamo.h"
#include "aws_dynamo_put_item.h"

//#define DEBUG_PARSER 1

enum {
	PARSER_STATE_NONE = 0,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_ATTRIBUTES_KEY,
	PARSER_STATE_ATTRIBUTES_MAP,
	PARSER_STATE_ATTRIBUTE_KEY,
	PARSER_STATE_ATTRIBUTE_MAP,
	PARSER_STATE_ATTRIBUTE_VALUE,
	PARSER_STATE_CAPACITY_KEY,
};

struct put_item_ctx {

	struct aws_dynamo_put_item_response *r;

	/* Index into the response structure. */
	int attribute_index;

	int parser_state;
};

static int put_item_number(void *ctx, const char *val, unsigned int len)
{
	struct put_item_ctx *_ctx = (struct put_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("put_item_number, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_CAPACITY_KEY:{
			if (aws_dynamo_json_get_double(val, len, &(_ctx->r->consumed_capacity_units)) == -1) {
				Warnx("put_item_number: failed to get capacity int.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	default:{
			Warnx("put_item_number - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("put_item_number exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int put_item_string(void *ctx, const unsigned char *val,
				 unsigned int len)
{
	struct put_item_ctx *_ctx = (struct put_item_ctx *)ctx;
	struct aws_dynamo_attribute *attribute;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("put_item_string, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	attribute = &(_ctx->r->attributes[_ctx->attribute_index]);

	switch (_ctx->parser_state) {
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			if (aws_dynamo_parse_attribute_value(attribute, val, len) != 1) {
				Warnx("get_item_string - attribute parse failed, attribute %d",
					_ctx->attribute_index);
				return 0;
			}
			break;
		}
	default:{
			Warnx("put_item_string - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("put_item_string exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int put_item_start_map(void *ctx)
{
	struct put_item_ctx *_ctx = (struct put_item_ctx *)ctx;

#ifdef DEBUG_PARSER
	Debug("put_item_start_map, enter state %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_NONE:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_ATTRIBUTES_KEY:{
			_ctx->parser_state = PARSER_STATE_ATTRIBUTES_MAP;
			break;
		}
	case PARSER_STATE_ATTRIBUTE_KEY:{
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_MAP;
			break;
		}
	default:{
			Warnx("put_item_start_map - unexpected state: %d",
			     _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("put_item_start_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int put_item_map_key(void *ctx, const unsigned char *val,
				  unsigned int len)
{
	struct put_item_ctx *_ctx = (struct put_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("put_item_map_key, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ATTRIBUTES, val, len)) {
				_ctx->parser_state = PARSER_STATE_ATTRIBUTES_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CONSUMED_CAPACITY, val, len)) {
				_ctx->parser_state = PARSER_STATE_CAPACITY_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("put_item_map_key: Unknown root key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_ATTRIBUTES_MAP:{
			/* Set the attribute index based on the name. */
			int attribute;

			for (attribute = 0; attribute < _ctx->r->num_attributes;
			     attribute++) {
				struct aws_dynamo_attribute *a =
				    &(_ctx->r->attributes[attribute]);

				if (len == a->name_len
				    && strncmp(val, a->name, len) == 0) {
					_ctx->attribute_index = attribute;
					break;
				}
			}

			if (attribute == _ctx->r->num_attributes) {
				char attr[len + 1];
				snprintf(attr, len + 1, "%s", val);

				Warnx("put_item_map_key: Unknown attribute '%s'.", attr);
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_KEY;
			break;
		}
	case PARSER_STATE_ATTRIBUTE_MAP:{
			/* verify the attribute is of the expected type. */
			struct aws_dynamo_attribute *a;
			const char *expected_type;

			a = &(_ctx->r->attributes[_ctx->attribute_index]);
			expected_type = aws_dynamo_attribute_types[a->type];

			if (len != strlen(expected_type)
			    || strncmp(val, expected_type, len) != 0) {
				char type[len + 1];
				snprintf(type, len + 1, "%s", val);

				Warnx("put_item_map_key: Unexpected type for attribute %s.  Got %s, expected %s.",
					a->name, type, expected_type);
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_VALUE;
			break;
		}
	default:{
			Warnx("put_item_map_key - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("put_item_map_key exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int put_item_end_map(void *ctx)
{
	struct put_item_ctx *_ctx = (struct put_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("put_item_end_map enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			_ctx->parser_state = PARSER_STATE_ATTRIBUTES_MAP;
			break;
		}
	case PARSER_STATE_ATTRIBUTES_MAP:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_ROOT_MAP:{
			_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
	default:{
			Warnx("put_item_end_map - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("put_item_end_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int put_item_start_array(void *ctx)
{
	struct put_item_ctx *_ctx = (struct put_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("put_item_start_array enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			/* A String Set or a Number Set, no need for a state change. */
			break;
		}
	default:{
			Warnx("put_item_start_array - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("put_item_start_array exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int put_item_end_array(void *ctx)
{
	struct put_item_ctx *_ctx = (struct put_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("put_item_end_array enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			/* A String Set or a Number Set, no need for a state change. */
			break;
		}
	default:{
			Warnx("put_item_end_array - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("put_item_end_array exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static yajl_callbacks put_item_callbacks = {
	.yajl_number = put_item_number,
	.yajl_string = put_item_string,
	.yajl_start_map = put_item_start_map,
	.yajl_map_key = put_item_map_key,
	.yajl_end_map = put_item_end_map,
	.yajl_start_array = put_item_start_array,
	.yajl_end_array = put_item_end_array,
};

struct aws_dynamo_put_item_response * aws_dynamo_parse_put_item_response(const char *response, int response_len, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	yajl_handle hand;
	yajl_status stat;
	struct put_item_ctx _ctx = { 0 };

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx("aws_dynamo_parse_put_item_response: response alloc failed.");
		return NULL;
	}

	if (num_attributes > 0) {
		_ctx.r->attributes = malloc(sizeof(*(_ctx.r->attributes)) * num_attributes);
		if (_ctx.r->attributes == NULL) {
			Warnx("aws_dynamo_parse_put_item_response: attribute alloc failed.");
			free(_ctx.r);
			return NULL;
		}
		memcpy(_ctx.r->attributes, attributes, sizeof(*(attributes)) * num_attributes);
		_ctx.r->num_attributes = num_attributes;
	}

	hand = yajl_alloc(&put_item_callbacks, NULL, NULL, &_ctx);

	yajl_parse(hand, response, response_len);

	stat = yajl_parse_complete(hand);

	if (stat != yajl_status_ok) {
		unsigned char *str =
		    yajl_get_error(hand, 1, response, response_len);
		Warnx("aws_dynamo_parse_put_item_response: json parse failed, '%s'", (const char *)str);
		yajl_free_error(hand, str);
		yajl_free(hand);
		aws_dynamo_free_put_item_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_dynamo_put_item_response *aws_dynamo_put_item(struct aws_handle *aws, const char *request, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	const char *response;
	int response_len;
	struct aws_dynamo_put_item_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_PUT_ITEM, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_put_item: Failed to get response.");
		return NULL;
	}

	if ((r = aws_dynamo_parse_put_item_response(response, response_len,
						      attributes, num_attributes)) == NULL) {
		Warnx("aws_dynamo_put_item: Failed to parse response.");
		return NULL;
	}

	return r;
}

void aws_dynamo_dump_put_item_response(struct aws_dynamo_put_item_response *r)
{
#ifdef DEBUG_PARSER
	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	Debug("consumed_capacity_units = %f", r->consumed_capacity_units);

	if (r->attributes != NULL) {
		aws_dynamo_dump_attributes(r->attributes, r->num_attributes);
	} else {
		Debug("no attributes");
	}
#endif
}

void aws_dynamo_free_put_item_response(struct aws_dynamo_put_item_response *r)
{
	if (r == NULL) {
		return;
	}

	if (r->attributes != NULL) {
		aws_dynamo_free_attributes(r->attributes, r->num_attributes);
	}

	free(r);	
}

