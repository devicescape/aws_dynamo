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
#include "aws_dynamo_scan.h"

enum {
	PARSER_STATE_NONE,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_CAPACITY_KEY,
	PARSER_STATE_COUNT_KEY,
	PARSER_STATE_ITEMS_KEY,
	PARSER_STATE_ITEMS_ARRAY,
	PARSER_STATE_ITEM_MAP,
	PARSER_STATE_SCANNED_COUNT_KEY,
	PARSER_STATE_ATTRIBUTE_KEY,
	PARSER_STATE_ATTRIBUTE_MAP,
	PARSER_STATE_ATTRIBUTE_VALUE,
	PARSER_STATE_LAST_EVALUATED_KEY,
	PARSER_STATE_LAST_EVALUATED_MAP,
	PARSER_STATE_LAST_EVALUATED_HASH_KEY_KEY,
	PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP,
	PARSER_STATE_LAST_EVALUATED_RANGE_KEY_KEY,
	PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP,
};

static const char *parser_state_strings[] = {
	"none",
	"root map",
	"capacity key",
	"count key",
	"items key",
	"items array",
	"item map",
	"scanned count key",
	"attribute key",
	"attribute map",
	"attribute value",
	"last evaluated",
	"last evaluated map",
	"last evaluated hash key key",
	"last evaluated hash key map",
	"last evaluated range key key",
	"last evaluated range key map",
};

static const char *parser_state_string(int state) {
	if (state < 0 || state > sizeof(parser_state_strings) / sizeof(parser_state_strings[0])) {
		return "invalid state";
	} else {
		return parser_state_strings[state];
	}
}

struct scan_ctx {

	/* Indicies into the response structure. */
	int item_index;
	int attribute_index;
	struct aws_dynamo_scan_response *r;

	/* These define the expected attributes for each item. */
	struct aws_dynamo_attribute *attributes; /* attribute template */
	int num_attributes; /* number of attributes for each item. */

	int parser_state;
};

static int scan_number(void * ctx, const char *val, unsigned int len)
{
	struct scan_ctx *_ctx = (struct scan_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("scan_number, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_COUNT_KEY: {
			if (aws_dynamo_json_get_int(val, len, &(_ctx->r->count)) == -1) {
				Warnx("scan_number: failed to get count int.");
				return 0;
			} else {
				/* Assume we can depend on getting the Count attribute before any Items.
					If this is an invalid assumption then we'll need to add new items using
					realloc. */

				struct aws_dynamo_item *items;

				items = calloc(sizeof(*items), _ctx->r->count);
				if (items == NULL) {
					Warnx("scan_number: item alloc failed.");
					return 0;
				} else {
					int item;

					_ctx->r->items = items;

					for (item = 0; item < _ctx->r->count; item++) {
						struct aws_dynamo_attribute *attributes;

						attributes = malloc(sizeof(*attributes) * _ctx->num_attributes);
						if (attributes == NULL) {
							Warnx("scan_number: attribute alloc failed.");
							for (item = item - 1; item >= 0; item--) {
								free(_ctx->r->items[item].attributes);
							}
							free(_ctx->r->items);	
							return 0;
						}

						/* Set expected types for attributes. */
						memcpy(attributes, _ctx->attributes,
							sizeof(*attributes) * _ctx->num_attributes);
						_ctx->r->items[item].attributes = attributes;
						_ctx->r->items[item].num_attributes = _ctx->num_attributes;
					}
					/* We haven't started any items yet. */
					_ctx->item_index = -1;
				}
			}
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		case PARSER_STATE_SCANNED_COUNT_KEY: {
			if (aws_dynamo_json_get_int(val, len, &(_ctx->r->scanned_count)) == -1) {
				Warnx("scan_number: failed to scanned count int.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		case PARSER_STATE_CAPACITY_KEY: {
			if (aws_dynamo_json_get_double(val, len, &(_ctx->r->consumed_capacity_units)) == -1) {
				Warnx("scan_number: failed to get capacity int.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		default: {
			Warnx("scan_number - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("scan_number exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int scan_string(void *ctx, const unsigned char *val,  unsigned int len)
{  
	struct scan_ctx *_ctx = (struct scan_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("scan_string, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_VALUE: {
			struct aws_dynamo_item *item;
			struct aws_dynamo_attribute *attribute;

			if (_ctx->r->items == NULL) {
				Warnx("scan_string - items is NULL, have we not gotten Count yet?");
				return 0;
			}

			if (_ctx->item_index == -1) {
				Warnx("scan_string - item_index is not set.");
				return 0;
			}

			item = &(_ctx->r->items[_ctx->item_index]);
			attribute = &(item->attributes[_ctx->attribute_index]);
			if (aws_dynamo_parse_attribute_value(attribute, val, len) != 1) {
				Warnx("scan_string - attribute parse failed, item %d, attribute %d",
					_ctx->item_index, _ctx->attribute_index);
				return 0;
			}
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP: {
			_ctx->r->hash_key->value = strndup(val, len);
			if (_ctx->r->hash_key->value == NULL) {
				Warnx("scan_string: failed to allocated last evaluated hash key value");
				return 0;
			}
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP: {
			_ctx->r->range_key->value = strndup(val, len);
			if (_ctx->r->range_key->value == NULL) {
				Warnx("scan_string: failed to allocated last evaluated range key value");
				return 0;
			}
			break;
		}
		default: {
			Warnx("scan_string - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("scan_string exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}  

static int scan_start_map(void *ctx)
{  
	struct scan_ctx *_ctx = (struct scan_ctx *) ctx;
	
#ifdef DEBUG_PARSER
	Debug("scan_start_map, enter state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_NONE: {
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		case PARSER_STATE_ITEMS_ARRAY: {
			if (_ctx->item_index >= _ctx->r->count - 1) {
				Warnx("scan_start_map: unexpected item count");
				return 0;
			}
			_ctx->item_index++;
			_ctx->parser_state = PARSER_STATE_ITEM_MAP;
			break;
		}
		case PARSER_STATE_ATTRIBUTE_KEY:{
				_ctx->parser_state = PARSER_STATE_ATTRIBUTE_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_KEY:{
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_KEY:{
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_KEY:{
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP;
			break;
		}
		default: {
			Warnx("scan_start_map - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("scan_start_map exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}
 
static int scan_map_key(void *ctx, const unsigned char *val,  
                            unsigned int len)  
{  
	struct scan_ctx *_ctx = (struct scan_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("scan_map_key, val = %s, enter state '%s'", buf, parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ROOT_MAP: {
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CONSUMED_CAPACITY, val, len)) {
				_ctx->parser_state = PARSER_STATE_CAPACITY_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_COUNT, val, len)) {
				_ctx->parser_state = PARSER_STATE_COUNT_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_SCANNED_COUNT, val, len)) {
				_ctx->parser_state = PARSER_STATE_SCANNED_COUNT_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ITEMS, val, len)) {
				_ctx->parser_state = PARSER_STATE_ITEMS_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_LAST_EVALUATED_KEY, val, len)) {
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_KEY;
			} else {
				Warnx("scan_map_key: Unknown key.");
				return 0;
			}
			break;
		}
		case PARSER_STATE_ITEM_MAP: {
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
				Warnx("scan_map_key: Unknown attribute.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_KEY;
			break;
		}
		case PARSER_STATE_ATTRIBUTE_MAP: {
			struct aws_dynamo_attribute *a = &(_ctx->attributes[_ctx->attribute_index]);
			const char *expected_type = aws_dynamo_attribute_types[a->type];

			if (len != strlen(expected_type) || strncmp(val, expected_type, len) != 0) {
				Warnx("scan_map_key: Unexpected attribute type.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_VALUE;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_MAP: {
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_HASH_KEY_ELEMENT, val, len)) {
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_HASH_KEY_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_RANGE_KEY_ELEMENT, val, len)) {
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_RANGE_KEY_KEY;
			} else {
				Warnx("scan_map_key: Unknown last eval key.");
				return 0;
			}
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP: {
			if (_ctx->r->hash_key != NULL) {
				Warnx("scan_map_key: duplicate last evaluated hash key?");
				return 0;
			}

			_ctx->r->hash_key = calloc(1, sizeof(*(_ctx->r->hash_key)));
			if (_ctx->r->hash_key == NULL) {
				Warnx("scan_map_key: failed to allocated last evaluated hash key");
				return 0;
			}

			_ctx->r->hash_key->type = strndup(val, len);
			if (_ctx->r->hash_key->type == NULL) {
				Warnx("scan_map_key: failed to allocated last evaluated hash key type");
				return 0;
			}
			
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP: {
			if (_ctx->r->range_key != NULL) {
				Warnx("scan_map_key: duplicate last evaluated range key?");
				return 0;
			}

			_ctx->r->range_key = calloc(1, sizeof(*(_ctx->r->range_key)));
			if (_ctx->r->range_key == NULL) {
				Warnx("scan_map_key: failed to allocated last evaluated range key");
				return 0;
			}

			_ctx->r->range_key->type = strndup(val, len);
			if (_ctx->r->range_key->type == NULL) {
				Warnx("scan_map_key: failed to allocated last evaluated range key type");
				return 0;
			}
			
			break;
		}
		default: {
			Warnx("scan_map_key - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("scan_map_key exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}  

static int scan_end_map(void *ctx)
{
	struct scan_ctx *_ctx = (struct scan_ctx *) ctx;
#ifdef DEBUG_PARSER
	Debug("scan_end_map enter state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_VALUE: {
				_ctx->parser_state = PARSER_STATE_ITEM_MAP;
			break;
		}
		case PARSER_STATE_ITEM_MAP: {
				_ctx->parser_state = PARSER_STATE_ITEMS_ARRAY;
			break;
		}
		case PARSER_STATE_ROOT_MAP:{
				_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP:{
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP:{
				_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_MAP:{
				_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		default: {
			Warnx("scan_map_end - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("scan_end_map exit state '%s'", parser_state_string(_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int scan_start_array(void *ctx)
{
	struct scan_ctx *_ctx = (struct scan_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("scan_start_array enter state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ITEMS_KEY:{
			_ctx->parser_state = PARSER_STATE_ITEMS_ARRAY;
			break;
		}
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			/* A String Set or a Number Set, no need for a state change. */
			break;
		}
	default:{
			Warnx("scan_start_array - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("scan_start_array exit state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static int scan_end_array(void *ctx)
{
	struct scan_ctx *_ctx = (struct scan_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("scan_end_array enter state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ITEMS_ARRAY:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			/* A String Set or a Number Set, no need for a state change. */
			break;
		}
	default:{
			Warnx("scan_end_array - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("scan_end_array exit state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static yajl_callbacks scan_callbacks = {  
	.yajl_number = scan_number,
	.yajl_string = scan_string,
	.yajl_start_map = scan_start_map,
	.yajl_map_key = scan_map_key,
	.yajl_end_map = scan_end_map,
	.yajl_start_array = scan_start_array,
	.yajl_end_array = scan_end_array,
};

struct aws_dynamo_scan_response *aws_dynamo_parse_scan_response(const char *response, int response_len,
	struct aws_dynamo_attribute *attributes, int num_attributes)
{
	yajl_handle hand;
	yajl_status stat;
	struct scan_ctx _ctx = {
		.num_attributes = num_attributes,
		.attributes = attributes,
	};

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx("aws_dynamo_parse_scan_response: alooc failed.");
		return NULL;
	}

#if YAJL_MAJOR == 2
	hand = yajl_alloc(&scan_callbacks, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_complete_parse(hand);
#else
	hand = yajl_alloc(&scan_callbacks, NULL, NULL, &_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_parse_complete(hand);
#endif

	if (stat != yajl_status_ok) {
		unsigned char * str = yajl_get_error(hand, 1, response, response_len);  
		Warnx("aws_dynamo_parse_scan_response: json parse failed, '%s'", (const char *) str);  
		yajl_free_error(hand, str);  
		yajl_free(hand);
		aws_dynamo_free_scan_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_dynamo_scan_response *aws_dynamo_scan(struct aws_handle *aws,
	const char *request, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	const char *response;
	int response_len;
	struct aws_dynamo_scan_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_SCAN, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_scan: Failed to get response.");
		return NULL; 
	}

	if ((r = aws_dynamo_parse_scan_response(response, response_len,
		attributes, num_attributes)) == NULL) {
		Warnx("aws_dynamo_scan: Failed to parse response.");
		return NULL; 
	}

	return r;
}

void aws_dynamo_dump_scan_response(struct aws_dynamo_scan_response *r) {
#ifdef DEBUG_PARSER
	int i;
	struct aws_dynamo_item *items;

	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	Debug("consumed_capacity_units = %f, item count = %d, scanned count = %d",
		r->consumed_capacity_units, r->count, r->scanned_count);

	items = r->items;
	for (i = 0; i < r->count; i++) {
		struct aws_dynamo_item *item = &(items[i]);
		Debug("Item %d, %d attributes", i, item->num_attributes);
		aws_dynamo_dump_attributes(item->attributes, item->num_attributes);

	}

	if (r->hash_key) {
		Debug("Last evaluated hash key: %s:%s", r->hash_key->type, r->hash_key->value);
	}

	if (r->range_key) {
		Debug("Last evaluated range key: %s:%s", r->range_key->type, r->range_key->value);
	}
#endif
}


void aws_dynamo_free_scan_response(struct aws_dynamo_scan_response *r) {
	int i;
	struct aws_dynamo_item *items;

	if (r == NULL) {
		return;
	}

	items = r->items;
	for (i = 0; i < r->count; i++) {
		struct aws_dynamo_item *item = &(items[i]);

		aws_dynamo_free_attributes(item->attributes, item->num_attributes);
	}

	if (r->hash_key) {
		free(r->hash_key->type);
		free(r->hash_key->value);
		free(r->hash_key);
	}

	if (r->range_key) {
		free(r->range_key->type);
		free(r->range_key->value);
		free(r->range_key);
	}

	free(r->items);
	free(r);	
}

