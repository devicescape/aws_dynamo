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
#include "aws_dynamo_query.h"

enum {
	PARSER_STATE_NONE,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_CAPACITY_KEY,
	PARSER_STATE_COUNT_KEY,
	PARSER_STATE_ITEMS_KEY,
	PARSER_STATE_ITEMS_ARRAY,
	PARSER_STATE_ITEM_MAP,
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

struct query_ctx {

	/* Indicies into the response structure. */
	int item_index;
	int attribute_index;
	struct aws_dynamo_query_response *r;

	/* These define the expected attributes for each item. */
	struct aws_dynamo_attribute *attributes; /* attribute template */
	int num_attributes; /* number of attributes for each item. */

	int parser_state;
};

static int query_number(void * ctx, const char *val, unsigned int len)
{
	struct query_ctx *q_ctx = (struct query_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("query_number, val = %s, enter state '%s'", buf, parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (q_ctx->parser_state) {
		case PARSER_STATE_COUNT_KEY: {
			if (aws_dynamo_json_get_int(val, len, &(q_ctx->r->count)) == -1) {
				Warnx("query_number: failed to get count int.");
				return 0;
			} else {
				/* Assume we can depend on getting the Count attribute before any Items.
					If this is an invalid assumption then we'll need to add new items using
					realloc. */

				struct aws_dynamo_item *items;

				items = calloc(sizeof(*items), q_ctx->r->count);
				if (items == NULL) {
					Warnx("query_number: item alloc failed.");
					return 0;
				} else {
					int item;

					q_ctx->r->items = items;

					for (item = 0; item < q_ctx->r->count; item++) {
						struct aws_dynamo_attribute *attributes;

						attributes = malloc(sizeof(*attributes) * q_ctx->num_attributes);
						if (attributes == NULL) {
							Warnx("query_number: attribute alloc failed.");
							for (item = item - 1; item >= 0; item--) {
								free(q_ctx->r->items[item].attributes);
							}
							free(q_ctx->r->items);	
							return 0;
						}

						/* Set expected types for attributes. */
						memcpy(attributes, q_ctx->attributes,
							sizeof(*attributes) * q_ctx->num_attributes);
						q_ctx->r->items[item].attributes = attributes;
						q_ctx->r->items[item].num_attributes = q_ctx->num_attributes;
					}
					/* We haven't started any items yet. */
					q_ctx->item_index = -1;
				}
			}
			q_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		case PARSER_STATE_CAPACITY_KEY: {
			if (aws_dynamo_json_get_double(val, len, &(q_ctx->r->consumed_capacity_units)) == -1) {
				Warnx("query_number: failed to get capacity int.");
				return 0;
			}
			q_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		default: {
			Warnx("query_number - unexpected state");
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("query_number exit '%s'", parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int query_string(void *ctx, const unsigned char *val,  unsigned int len)
{  
	struct query_ctx *q_ctx = (struct query_ctx *) ctx;
	struct aws_dynamo_item *item;
	struct aws_dynamo_attribute *attribute;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("query_string, val = %s, enter state '%s'", buf, parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	if (q_ctx->r->items == NULL) {
		Warnx("query_string - items is NULL, have we not gotten Count yet?");
		return 0;
	}

	item = &(q_ctx->r->items[q_ctx->item_index]);
	attribute = &(item->attributes[q_ctx->attribute_index]);

	switch (q_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_VALUE: {
			if (aws_dynamo_parse_attribute_value(attribute, val, len) != 1) {
				Warnx("query_string - attribute parse failed, item %d, attribute %d",
					q_ctx->item_index, q_ctx->attribute_index);
				return 0;
			}
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP: {
			q_ctx->r->hash_key->value = strndup(val, len);
			if (q_ctx->r->hash_key->value == NULL) {
				Warnx("query_string: failed to allocated last evaluated hash key value");
				return 0;
			}
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP: {
			q_ctx->r->range_key->value = strndup(val, len);
			if (q_ctx->r->range_key->value == NULL) {
				Warnx("query_string: failed to allocated last evaluated range key value");
				return 0;
			}
			break;
		}
		default: {
			Warnx("query_string - unexpected state");
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("query_string exit '%s'", parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}  

static int query_start_map(void *ctx)
{  
	struct query_ctx *q_ctx = (struct query_ctx *) ctx;
	
#ifdef DEBUG_PARSER
	Debug("query_start_map, enter state '%s'", parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (q_ctx->parser_state) {
		case PARSER_STATE_NONE: {
			q_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		case PARSER_STATE_ITEMS_ARRAY: {
			if (q_ctx->item_index >= q_ctx->r->count - 1) {
				Warnx("query_start_map: unexpected item count");
				return 0;
			}
			q_ctx->item_index++;
			q_ctx->parser_state = PARSER_STATE_ITEM_MAP;
			break;
		}
		case PARSER_STATE_ATTRIBUTE_KEY:{
				q_ctx->parser_state = PARSER_STATE_ATTRIBUTE_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_KEY:{
				q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_KEY:{
				q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_KEY:{
				q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP;
			break;
		}
		default: {
			Warnx("query_start_map - unexpected state: '%s'", parser_state_string(q_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("query_start_map exit '%s'", parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}
 
static int query_map_key(void *ctx, const unsigned char *val,  
                            unsigned int len)  
{  
	struct query_ctx *q_ctx = (struct query_ctx *) ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);
	
	Debug("query_map_key, val = %s, enter state '%s'", buf, parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (q_ctx->parser_state) {
		case PARSER_STATE_ROOT_MAP: {
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CONSUMED_CAPACITY, val, len)) {
				q_ctx->parser_state = PARSER_STATE_CAPACITY_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_COUNT, val, len)) {
				q_ctx->parser_state = PARSER_STATE_COUNT_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ITEMS, val, len)) {
				q_ctx->parser_state = PARSER_STATE_ITEMS_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_LAST_EVALUATED_KEY, val, len)) {
				q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_KEY;
			} else {
				Warnx("query_map_key: Unknown key.");
				return 0;
			}
			break;
		}
		case PARSER_STATE_ITEM_MAP: {
			/* Set the attribute index based on the name. */
			int attribute;

			for (attribute = 0; attribute < q_ctx->num_attributes; attribute++) {
				struct aws_dynamo_attribute *a = &(q_ctx->attributes[attribute]);

				if (len == a->name_len && strncmp(val, a->name, len) == 0) {
					q_ctx->attribute_index = attribute;
					break;
				}
			}

			if (attribute == q_ctx->num_attributes) {
				Warnx("query_map_key: Unknown attribute.");
				return 0;
			}
			q_ctx->parser_state = PARSER_STATE_ATTRIBUTE_KEY;
			break;
		}
		case PARSER_STATE_ATTRIBUTE_MAP: {
			struct aws_dynamo_attribute *a = &(q_ctx->attributes[q_ctx->attribute_index]);
			const char *expected_type = aws_dynamo_attribute_types[a->type];

			if (len != strlen(expected_type) || strncmp(val, expected_type, len) != 0) {
				Warnx("query_map_key: Unexpected attribute type.");
				return 0;
			}
			q_ctx->parser_state = PARSER_STATE_ATTRIBUTE_VALUE;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_MAP: {
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_HASH_KEY_ELEMENT, val, len)) {
				q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_HASH_KEY_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_RANGE_KEY_ELEMENT, val, len)) {
				q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_RANGE_KEY_KEY;
			} else {
				Warnx("query_map_key: Unknown last eval key.");
				return 0;
			}
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP: {
			if (q_ctx->r->hash_key != NULL) {
				Warnx("query_map_key: duplicate last evaluated hash key?");
				return 0;
			}

			q_ctx->r->hash_key = calloc(1, sizeof(*(q_ctx->r->hash_key)));
			if (q_ctx->r->hash_key == NULL) {
				Warnx("query_map_key: failed to allocated last evaluated hash key");
				return 0;
			}

			q_ctx->r->hash_key->type = strndup(val, len);
			if (q_ctx->r->hash_key->type == NULL) {
				Warnx("query_map_key: failed to allocated last evaluated hash key type");
				return 0;
			}
			
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP: {
			if (q_ctx->r->range_key != NULL) {
				Warnx("query_map_key: duplicate last evaluated range key?");
				return 0;
			}

			q_ctx->r->range_key = calloc(1, sizeof(*(q_ctx->r->range_key)));
			if (q_ctx->r->range_key == NULL) {
				Warnx("query_map_key: failed to allocated last evaluated range key");
				return 0;
			}

			q_ctx->r->range_key->type = strndup(val, len);
			if (q_ctx->r->range_key->type == NULL) {
				Warnx("query_map_key: failed to allocated last evaluated range key type");
				return 0;
			}
			
			break;
		}
		default: {
			Warnx("query_map_key - unexpected state");
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("query_map_key exit '%s'", parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */
	return 1;
}  

static int query_end_map(void *ctx)
{
	struct query_ctx *q_ctx = (struct query_ctx *) ctx;
#ifdef DEBUG_PARSER
	Debug("query_end_map enter '%s'", parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	switch (q_ctx->parser_state) {
		case PARSER_STATE_ATTRIBUTE_VALUE: {
				q_ctx->parser_state = PARSER_STATE_ITEM_MAP;
			break;
		}
		case PARSER_STATE_ITEM_MAP: {
				q_ctx->parser_state = PARSER_STATE_ITEMS_ARRAY;
			break;
		}
		case PARSER_STATE_ROOT_MAP:{
				q_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_HASH_KEY_MAP:{
			q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_RANGE_KEY_MAP:{
			q_ctx->parser_state = PARSER_STATE_LAST_EVALUATED_MAP;
			break;
		}
		case PARSER_STATE_LAST_EVALUATED_MAP:{
			q_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
		default: {
			Warnx("query_end_map - unexpected state");
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("query_end_map exit '%s'", parser_state_string(q_ctx->parser_state));
#endif /* DEBUG_PARSER */

	return 1;
}

static int query_start_array(void *ctx)
{
	struct query_ctx *_ctx = (struct query_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("query_start_array enter state '%s'", parser_state_string(_ctx->parser_state));
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
			Warnx("query_start_array - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("query_start_array exit state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static int query_end_array(void *ctx)
{
	struct query_ctx *_ctx = (struct query_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("query_end_array enter state '%s'", parser_state_string(_ctx->parser_state));
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
			Warnx("query_end_array - unexpected state '%s'", parser_state_string(_ctx->parser_state));
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("query_end_array exit state '%s'", parser_state_string(_ctx->parser_state));
#endif				/* DEBUG_PARSER */

	return 1;
}

static yajl_callbacks query_callbacks = {  
	.yajl_number = query_number,
	.yajl_string = query_string,
	.yajl_start_map = query_start_map,
	.yajl_map_key = query_map_key,
	.yajl_end_map = query_end_map,
	.yajl_start_array = query_start_array,
	.yajl_end_array = query_end_array,
};

struct aws_dynamo_query_response *aws_dynamo_parse_query_response(const char *response, int response_len,
	struct aws_dynamo_attribute *attributes, int num_attributes)
{
	yajl_handle hand;
	yajl_status stat;
	struct query_ctx q_ctx = {
		.num_attributes = num_attributes,
		.attributes = attributes,
	};

	q_ctx.r = calloc(sizeof(*(q_ctx.r)), 1);
	if (q_ctx.r == NULL) {
		Warnx("aws_dynamo_parse_query_response: alooc failed.");
		return NULL;
	}

#if YAJL_MAJOR == 2
	hand = yajl_alloc(&query_callbacks, NULL, &q_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_complete_parse(hand);
#else
	hand = yajl_alloc(&query_callbacks, NULL, NULL, &q_ctx);
	yajl_parse(hand, response, response_len);
	stat = yajl_parse_complete(hand);
#endif

	if (stat != yajl_status_ok) {
		unsigned char * str = yajl_get_error(hand, 1, response, response_len);  
		Warnx("aws_dynamo_parse_query_response: json parse failed, '%s'", (const char *) str);  
		yajl_free_error(hand, str);  
		yajl_free(hand);
		aws_dynamo_free_query_response(q_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return q_ctx.r;
}

struct aws_dynamo_query_response *aws_dynamo_query(struct aws_handle *aws,
	const char *request, struct aws_dynamo_attribute *attributes, int num_attributes)
{
	const char *response;
	int response_len;
	struct aws_dynamo_query_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_QUERY, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_query: Failed to get response.");
		return NULL; 
	}

	if ((r = aws_dynamo_parse_query_response(response, response_len,
		attributes, num_attributes)) == NULL) {
		Warnx("aws_dynamo_query: Failed to parse response.");
		return NULL; 
	}

	return r;
}

struct aws_dynamo_query_response *aws_dynamo_query_combine_and_free_responses(
					 struct aws_dynamo_query_response *current,
					 struct aws_dynamo_query_response *next) {
	struct aws_dynamo_query_response *r;

	r = calloc(1, sizeof(*r));
	if (r == NULL) {
		Warnx("aws_dynamo_query_combine_and_free_responses: calloc() failed");
		return NULL;
	}

	r->consumed_capacity_units = current->consumed_capacity_units + next->consumed_capacity_units;
	r->count = current->count + next->count;
	r->items = realloc(current->items, sizeof(*(current->items)) * (r->count));
	if (r->items == NULL) {
		Warnx("aws_dynamo_query_combine_and_free_responses: realloc() failed");
		free(r);
		return NULL;
	}

	/* copy items from 'next' to the new array. */
  	memcpy(r->items + current->count, next->items, next->count * sizeof(*(next->items)));	

	r->hash_key = next->hash_key;
	r->range_key = next->range_key;

	/* Free the pieces we aren't using. */
	free(next->items);
	free(next);

	if (current->hash_key) {
		free(current->hash_key->type);
		free(current->hash_key->value);
		free(current->hash_key);
	}

	if (current->range_key) {
		free(current->range_key->type);
		free(current->range_key->value);
		free(current->range_key);
	}
	free(current);

	return r;
}

void aws_dynamo_dump_query_response(struct aws_dynamo_query_response *r) {
#ifdef DEBUG_PARSER
	int i;
	struct aws_dynamo_item *items;

	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	Debug("consumed_capacity_units = %f, item count = %d",
		r->consumed_capacity_units, r->count);

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


void aws_dynamo_free_query_response(struct aws_dynamo_query_response *r) {
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

