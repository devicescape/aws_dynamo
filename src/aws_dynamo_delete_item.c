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
#include "aws_dynamo_delete_item.h"

//#define DEBUG_PARSER 1

enum {
	PARSER_STATE_NONE = 0,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_ATTRIBUTES_KEY,
	PARSER_STATE_ATTRIBUTES_MAP,
	PARSER_STATE_ATTRIBUTE_KEY,
	PARSER_STATE_ATTRIBUTE_MAP,
	PARSER_STATE_ATTRIBUTE_TYPE_KEY,
	PARSER_STATE_ATTRIBUTE_VALUE_ARRAY,
	PARSER_STATE_CONSUMED_CAPACITY_KEY,
};

static const char *parser_state_strings[] = {
	"none",
	"root map",
	"attributes key",
	"attributes map",
	"attribute key",
	"attribute map",
	"attribute type key",
	"attribute value array",
	"consumed capacity key",
};

static const char *parser_state_string(int state) {
	if (state < 0 || state > sizeof(parser_state_strings) / sizeof(parser_state_strings[0])) {
		return "invalid state";
	} else {
		return parser_state_strings[state];
	}
}

struct ctx {

	/* Indicies into the response structure. */
	int attribute_index;
	struct aws_dynamo_delete_item_response *r;

	/* These define the expected attributes. */
	struct aws_dynamo_attribute *attributes; /* attribute template */
	int num_attributes; /* number of attributes. */

	int parser_state;
};

static int handle_number(void * ctx, const char *val, unsigned int len)
{
	struct ctx *_ctx = (struct ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("handle_number, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_CONSUMED_CAPACITY_KEY: {
			if (aws_dynamo_json_get_double(val, len, &(_ctx->r->consumed_capacity_units)) == -1) {
				Warnx("handle_number: failed to get capacity int.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		default: {
			Warnx("handle_number - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_number exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int handle_string(void *ctx, const unsigned char *val,  unsigned int len)
{  
	struct ctx *_ctx = (struct ctx *) ctx;
	struct aws_dynamo_item *item;
	struct aws_dynamo_attribute *attribute;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("handle_string, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	item = &(_ctx->r->item);
	attribute = &(item->attributes[_ctx->attribute_index]);

	switch (_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_TYPE_KEY: {
			if (aws_dynamo_parse_attribute_value(attribute, val, len) != 1) {
				Warnx("handle_string - attribute parse failed, attribute %d",
					_ctx->attribute_index);
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_MAP;
			break;
		}
		case PARSER_STATE_ATTRIBUTE_VALUE_ARRAY: {
			if (aws_dynamo_parse_attribute_value(attribute, val, len) != 1) {
				Warnx("handle_string - attribute parse failed, attribute %d",
					_ctx->attribute_index);
				return 0;
			}
			break;
		}
		default: {
			Warnx("handle_string - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_string exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}  

static int handle_start_map(void *ctx)
{  
	struct ctx *_ctx = (struct ctx *) ctx;
	
#ifdef DEBUG_PARSER
	Debug("handle_start_map, enter state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_NONE: {
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		case PARSER_STATE_ATTRIBUTES_KEY: {
			_ctx->parser_state = PARSER_STATE_ATTRIBUTES_MAP;
			break;
		}
		case PARSER_STATE_ATTRIBUTE_KEY: {
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_MAP;
			break;
		}
		default: {
			Warnx("handle_start_map - unexpected state: %s", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_start_map exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}
 
static int handle_map_key(void *ctx, const unsigned char *val,  
                            unsigned int len)  
{  
	struct ctx *_ctx = (struct ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("handle_map_key, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ROOT_MAP: {
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CONSUMED_CAPACITY, val, len)) {
				_ctx->parser_state = PARSER_STATE_CONSUMED_CAPACITY_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ATTRIBUTES, val, len)) {
				_ctx->parser_state = PARSER_STATE_ATTRIBUTES_KEY;

				_ctx->r->item.attributes = malloc(sizeof(*(_ctx->attributes)) * _ctx->num_attributes);
				if (_ctx->r->item.attributes == NULL) {
					Warnx("handle_map_key: attribute alloc failed.");
					return 0;
				}

				/* Set expected types for attributes. */
				memcpy(_ctx->r->item.attributes, _ctx->attributes,
					sizeof(*(_ctx->attributes)) * _ctx->num_attributes);
				_ctx->r->item.num_attributes = _ctx->num_attributes;

			} else {
				Warnx("handle_map_key: Unknown key.");
				return 0;
			}
			break;
		}
		case PARSER_STATE_ATTRIBUTES_MAP: {
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
				Warnx("handle_map_key: Unknown attribute.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_KEY;
			break;
		}
		case PARSER_STATE_ATTRIBUTE_MAP: {
			struct aws_dynamo_attribute *a = &(_ctx->attributes[_ctx->attribute_index]);
			const char *expected_type = aws_dynamo_attribute_types[a->type];

			if (len != strlen(expected_type) || strncmp(val, expected_type, len) != 0) {
				Warnx("handle_map_key: Unexpected attribute type.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_TYPE_KEY;
			break;
		}
		default: {
			Warnx("handle_map_key - unexpected state: %s", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_map_key exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}  

static int handle_end_map(void *ctx)
{
	struct ctx *_ctx = (struct ctx *) ctx;
#ifdef DEBUG_PARSER
	Debug("handle_end_map enter state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	switch (_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_MAP: {
				_ctx->parser_state = PARSER_STATE_ATTRIBUTES_MAP;
			break;
		}
		case PARSER_STATE_ATTRIBUTES_MAP: {
				_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		case PARSER_STATE_ROOT_MAP: {
				_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
		default: {
			Warnx("handle_end_map - unexpected state: %s", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}
#ifdef DEBUG_PARSER
	Debug("handle_end_map exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int handle_start_array(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_start_array enter state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_TYPE_KEY: {
				_ctx->parser_state = PARSER_STATE_ATTRIBUTE_VALUE_ARRAY;
			break;
		}
	default:{
			Warnx("handle_start_array - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_start_array exit state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_end_array(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_end_array enter state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_VALUE_ARRAY: {
				_ctx->parser_state = PARSER_STATE_ATTRIBUTE_MAP;
			break;
		}
	default:{
			Warnx("handle_end_array - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_end_array exit state '%s'", parser_state_string(_ctx->parser_state));
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

struct aws_dynamo_delete_item_response *aws_dynamo_parse_delete_item_response(const char *response,
	int response_len, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	yajl_handle hand;
	yajl_status stat;
	struct ctx _ctx = {
		.num_attributes = num_attributes,
		.attributes = attributes,
	};

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx("aws_dynamo_parse_delete_item_response: alloc failed.");
		return NULL;
	}

#if YAJL_MAJOR == 2
	hand = yajl_alloc(&handle_callbacks, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_complete_parse(hand);
#else
	hand = yajl_alloc(&handle_callbacks, NULL, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_parse_complete(hand);
#endif

	if (stat != yajl_status_ok) {
		unsigned char * str = yajl_get_error(hand, 1, response, response_len);  
		Warnx("aws_dynamo_parse_delete_item_response: json parse failed, '%s'", (const char *) str);  
		yajl_free_error(hand, str);  
		yajl_free(hand);
		aws_dynamo_free_delete_item_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_dynamo_delete_item_response *aws_dynamo_delete_item(struct aws_handle *aws,
	const char *request, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	const char *response;
	int response_len;
	struct aws_dynamo_delete_item_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_DELETE_ITEM, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_delete_item: Failed to get response.");
		return NULL; 
	}

	if ((r = aws_dynamo_parse_delete_item_response(response, response_len,
		attributes, num_attributes)) == NULL) {
		Warnx("aws_dynamo_delete_item: Failed to parse response.");
		return NULL; 
	}

	return r;
}

void aws_dynamo_dump_delete_item_response(struct aws_dynamo_delete_item_response *r) {
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

void aws_dynamo_free_delete_item_response(struct aws_dynamo_delete_item_response *r) {
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

