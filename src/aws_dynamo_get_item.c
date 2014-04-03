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

#include <stdlib.h>

#include <yajl/yajl_parse.h>

#include "http.h"
#include "aws_dynamo.h"
#include "aws_dynamo_get_item.h"

//#define DEBUG_PARSER 1

#define GET_ITEM_PARSER_STATE_NONE					0
#define GET_ITEM_PARSER_STATE_ROOT					1
#define GET_ITEM_PARSER_STATE_CAPACITY				2
#define GET_ITEM_PARSER_STATE_ITEM					3
#define GET_ITEM_PARSER_STATE_ATTRIBUTES			4
#define GET_ITEM_PARSER_STATE_ATTRIBUTE			5
#define GET_ITEM_PARSER_STATE_ATTRIBUTE_VALUE	6

struct get_item_ctx {

	/* Indicies into the response structure. */
	int attribute_index;
	struct aws_dynamo_get_item_response *r;

	/* These define the expected attributes. */
	struct aws_dynamo_attribute *attributes; /* attribute template */
	int num_attributes; /* number of attributes. */

	int parser_state;
};

static int get_item_number(void * ctx, const char *val, unsigned int len)
{
	struct get_item_ctx *_ctx = (struct get_item_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("get_item_number, val = %s, enter state %d", buf, _ctx->parser_state);
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case GET_ITEM_PARSER_STATE_CAPACITY: {
			if (aws_dynamo_json_get_double(val, len, &(_ctx->r->consumed_capacity_units)) == -1) {
				Warnx("get_item_number: failed to get capacity int.");
				return 0;
			}
			_ctx->parser_state = GET_ITEM_PARSER_STATE_ROOT;
			break;
		}
		default: {
			Warnx("get_item_number - unexpected state");
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("get_item_number exit %d", _ctx->parser_state);
#endif /* DEBUG_PARSER */

	return 1;
}

static int get_item_string(void *ctx, const unsigned char *val,  unsigned int len)
{  
	struct get_item_ctx *_ctx = (struct get_item_ctx *) ctx;
	struct aws_dynamo_item *item;
	struct aws_dynamo_attribute *attribute;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("get_item_string, val = %s, enter state %d", buf, _ctx->parser_state);
#endif /* DEBUG_PARSER */

	item = &(_ctx->r->item);
	attribute = &(item->attributes[_ctx->attribute_index]);

	switch (_ctx->parser_state) {
		case GET_ITEM_PARSER_STATE_ATTRIBUTE_VALUE: {
			if (aws_dynamo_parse_attribute_value(attribute, val, len) != 1) {
				Warnx("get_item_string - attribute parse failed, attribute %d",
					_ctx->attribute_index);
				return 0;
			}
			break;
		}
		default: {
			Warnx("get_item_string - unexpected state");
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("get_item_string exit %d", _ctx->parser_state);
#endif /* DEBUG_PARSER */

	return 1;
}  

static int get_item_start_map(void *ctx)
{  
	struct get_item_ctx *_ctx = (struct get_item_ctx *) ctx;
	
#ifdef DEBUG_PARSER
	Debug("get_item_start_map, enter state %d", _ctx->parser_state);
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case GET_ITEM_PARSER_STATE_NONE: {
			_ctx->parser_state = GET_ITEM_PARSER_STATE_ROOT;
			break;
		}
		case GET_ITEM_PARSER_STATE_ITEM: {
			_ctx->parser_state = GET_ITEM_PARSER_STATE_ATTRIBUTES;
			break;
		}
		case GET_ITEM_PARSER_STATE_ATTRIBUTE: {
			_ctx->parser_state = GET_ITEM_PARSER_STATE_ATTRIBUTE_VALUE;
			break;
		}
		default: {
			Warnx("get_item_start_map - unexpected state: %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("get_item_start_map exit %d", _ctx->parser_state);
#endif /* DEBUG_PARSER */
	return 1;
}
 
static int get_item_map_key(void *ctx, const unsigned char *val,  
                            unsigned int len)  
{  
	struct get_item_ctx *_ctx = (struct get_item_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("get_item_map_key, val = %s, enter state %d", buf, _ctx->parser_state);
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case GET_ITEM_PARSER_STATE_ROOT: {
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CONSUMED_CAPACITY, val, len)) {
				_ctx->parser_state = GET_ITEM_PARSER_STATE_CAPACITY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ITEM, val, len)) {
				_ctx->parser_state = GET_ITEM_PARSER_STATE_ITEM;

				_ctx->r->item.attributes = malloc(sizeof(*(_ctx->attributes)) * _ctx->num_attributes);
				if (_ctx->r->item.attributes == NULL) {
					Warnx("get_item_map_key: attribute alloc failed.");
					return 0;
				}

				/* Set expected types for attributes. */
				memcpy(_ctx->r->item.attributes, _ctx->attributes,
					sizeof(*(_ctx->attributes)) * _ctx->num_attributes);
				_ctx->r->item.num_attributes = _ctx->num_attributes;

			} else {
				Warnx("get_item_map_key: Unknown key.");
				return 0;
			}
			break;
		}
		case GET_ITEM_PARSER_STATE_ATTRIBUTES: {
			/* Set the attribute index based on the name. */
			int attribute;

			for (attribute = 0; attribute < _ctx->num_attributes; attribute++) {
				struct aws_dynamo_attribute *a = &(_ctx->attributes[attribute]);

				if (len == a->name_len && strncmp(val, a->name, len) == 0) {
					_ctx->attribute_index = attribute;
					break;
				}
			}

			if (attribute == _ctx->num_attributes) {
				Warnx("get_item_map_key: Unknown attribute.");
				return 0;
			}
			_ctx->parser_state = GET_ITEM_PARSER_STATE_ATTRIBUTE;
			break;
		}
		case GET_ITEM_PARSER_STATE_ATTRIBUTE_VALUE: {
			struct aws_dynamo_attribute *a = &(_ctx->attributes[_ctx->attribute_index]);
			const char *expected_type = aws_dynamo_attribute_types[a->type];

			if (len != strlen(expected_type) || strncmp(val, expected_type, len) != 0) {
				Warnx("get_item_map_key: Unexpected attribute type.");
				return 0;
			}
			break;
		}
		default: {
			Warnx("get_item_map_key - unexpected state");
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("get_item_map_key exit %d", _ctx->parser_state);
#endif /* DEBUG_PARSER */
	return 1;
}  

static int get_item_end_map(void *ctx)
{
	struct get_item_ctx *_ctx = (struct get_item_ctx *) ctx;
#ifdef DEBUG_PARSER
	Debug("get_item_end_map enter %d", _ctx->parser_state);
#endif /* DEBUG_PARSER */
	switch (_ctx->parser_state) {
		case GET_ITEM_PARSER_STATE_ATTRIBUTE_VALUE: {
				_ctx->parser_state = GET_ITEM_PARSER_STATE_ATTRIBUTES;
			break;
		}
		case GET_ITEM_PARSER_STATE_ATTRIBUTES: {
				_ctx->parser_state = GET_ITEM_PARSER_STATE_ROOT;
			break;
		}
		case GET_ITEM_PARSER_STATE_CAPACITY: {
				_ctx->parser_state = GET_ITEM_PARSER_STATE_ROOT;
			break;
		}
		case GET_ITEM_PARSER_STATE_ROOT: {
				_ctx->parser_state = GET_ITEM_PARSER_STATE_NONE;
			break;
		}
		default: {
			Warnx("get_item_end_map - unexpected state");
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("get_item_end_map exit %d", _ctx->parser_state);
#endif /* DEBUG_PARSER */

	return 1;
}

static yajl_callbacks get_item_callbacks = {  
	.yajl_number = get_item_number,
	.yajl_string = get_item_string,
	.yajl_start_map = get_item_start_map,
	.yajl_map_key = get_item_map_key,
	.yajl_end_map = get_item_end_map,
};

struct aws_dynamo_get_item_response *aws_dynamo_parse_get_item_response(const char *response,
	int response_len, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	yajl_handle hand;
	yajl_status stat;
	struct get_item_ctx _ctx = {
		.num_attributes = num_attributes,
		.attributes = attributes,
	};

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx("aws_dynamo_parse_get_item_response: alloc failed.");
		return NULL;
	}

#if YAJL_MAJOR == 2
	hand = yajl_alloc(&get_item_callbacks, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_complete_parse(hand);
#else
	hand = yajl_alloc(&get_item_callbacks, NULL, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_parse_complete(hand);
#endif

	if (stat != yajl_status_ok) {
		unsigned char * str = yajl_get_error(hand, 1, response, response_len);  
		Warnx("aws_dynamo_parse_get_item_response: json parse failed, '%s'", (const char *) str);  
		yajl_free_error(hand, str);  
		yajl_free(hand);
		aws_dynamo_free_get_item_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_dynamo_get_item_response *aws_dynamo_get_item(struct aws_handle *aws,
	const char *request, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	const char *response;
	int response_len;
	struct aws_dynamo_get_item_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_GET_ITEM, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_get_item: Failed to get response.");
		return NULL; 
	}

	if ((r = aws_dynamo_parse_get_item_response(response, response_len,
		attributes, num_attributes)) == NULL) {
		Warnx("aws_dynamo_get_item: Failed to parse response.");
		return NULL; 
	}

	return r;
}

void aws_dynamo_dump_get_item_response(struct aws_dynamo_get_item_response *r) {
#ifdef DEBUG_PARSER

	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	Debug("consumed_capacity_units = %f", r->consumed_capacity_units);
	if (r->item.attributes != NULL) {
		aws_dynamo_dump_attributes(r->item.attributes, r->item.num_attributes);
	} else {
		Debug("no attributes");
	}
#endif
}

void aws_dynamo_free_get_item_response(struct aws_dynamo_get_item_response *r) {
	struct aws_dynamo_item *item;

	if (r == NULL) {
		return;
	}

	item = &(r->item);

	if (item->attributes != NULL) {
		aws_dynamo_free_attributes(item->attributes, item->num_attributes);
	}

	free(r);	
}

