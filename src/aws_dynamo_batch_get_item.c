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
#include "aws_dynamo_batch_get_item.h"

// TODO: Add support for "UnprocessedKeys".  The parse succeeds now when the
// keys are empty but anything inside the unprocessed keys will trigger an
// error.  We should have some way of storing the unprocessed keys so that
// they can be used in a subsequant request.

//#define DEBUG_PARSER 1

enum {
	PARSER_STATE_NONE,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_RESPONSES_KEY,
	PARSER_STATE_CAPACITY_KEY,
	PARSER_STATE_RESPONSES_MAP,
	PARSER_STATE_TABLE_KEY,
	PARSER_STATE_TABLE_MAP,
	PARSER_STATE_ITEMS_KEY,
	PARSER_STATE_ITEMS_ARRAY,
	PARSER_STATE_ITEM_MAP,
	PARSER_STATE_ATTRIBUTE_KEY,
	PARSER_STATE_ATTRIBUTE_MAP,
	PARSER_STATE_ATTRIBUTE_VALUE,
	PARSER_STATE_UNPROCESSED_KEY,
	PARSER_STATE_UNPROCESSED_MAP,
};

struct batch_get_item_ctx {

	/* This is the response structure that is built as the JSON
		is parsed. */
	struct aws_dynamo_batch_get_item_response *r;

	/* Indicies into the response structure. */
	int table_index;
	int item_index;
	int attribute_index;

	/* These define the expected tables and attributes. */
	struct aws_dynamo_batch_get_item_response_table *tables;
	int num_tables;

	int parser_state;
};

static int batch_get_item_number(void *ctx, const char *val, unsigned int len)
{
	struct batch_get_item_ctx *_ctx = (struct batch_get_item_ctx *)ctx;
	struct aws_dynamo_batch_get_item_response_table *table;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("batch_get_item_number, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	table = &(_ctx->r->tables[_ctx->table_index]);

	switch (_ctx->parser_state) {
	case PARSER_STATE_CAPACITY_KEY:{
			if (aws_dynamo_json_get_double(val, len, &(table->consumed_capacity_units)) == -1) {
				Warnx("batch_get_item_number: failed to get capacity int.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_TABLE_MAP;
			break;
		}
	default:{
			Warnx("batch_get_item_number - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("batch_get_item_number exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int batch_get_item_string(void *ctx, const unsigned char *val,
				 unsigned int len)
{
	struct batch_get_item_ctx *_ctx = (struct batch_get_item_ctx *)ctx;
	struct aws_dynamo_batch_get_item_response_table *table;
	struct aws_dynamo_item *item;
	struct aws_dynamo_attribute *attribute;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("batch_get_item_string, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	table = &(_ctx->r->tables[_ctx->table_index]);
	item = &(table->items[_ctx->item_index]);
	attribute = &(item->attributes[_ctx->attribute_index]);

	switch (_ctx->parser_state) {
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			if (aws_dynamo_parse_attribute_value(attribute, val, len) != 1) {
				Warnx("get_item_string - attribute parse failed, table %d (%s) item %d, attribute %d",
					_ctx->table_index, table->name, _ctx->item_index, _ctx->attribute_index);
				return 0;
			}
			break;
		}
	default:{
			Warnx("batch_get_item_string - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("batch_get_item_string exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int batch_get_item_start_map(void *ctx)
{
	struct batch_get_item_ctx *_ctx = (struct batch_get_item_ctx *)ctx;

#ifdef DEBUG_PARSER
	Debug("batch_get_item_start_map, enter state %d", _ctx->parser_state);
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
	case PARSER_STATE_TABLE_KEY:{
			_ctx->parser_state = PARSER_STATE_TABLE_MAP;
			break;
		}
	case PARSER_STATE_ITEMS_ARRAY:{
			/* This is the start of a new item. */
			struct aws_dynamo_batch_get_item_response_table *table;
			struct aws_dynamo_item *items;
			struct aws_dynamo_attribute *attributes;

			table = &(_ctx->r->tables[_ctx->table_index]);

			items = calloc(_ctx->item_index + 2, sizeof(*items));

			if (items == NULL) {
				Warnx("batch_get_item_start_map - item alloc failed");
				return 0;
			}

			memcpy(items, table->items,
			       sizeof(*items) * (_ctx->item_index + 1));

			attributes =
			    malloc(sizeof(*attributes) * table->num_attributes);
			if (attributes == NULL) {
				Warnx("batch_get_item_start_map - attributes alloc failed");
				free(items);
				return 0;
			}
			_ctx->item_index++;

			/* Set expected types for attributes. */
			memcpy(attributes, table->attributes,
			       sizeof(*attributes) * table->num_attributes);
			free(table->items);
			table->items = items;
			table->num_items++;
			table->items[_ctx->item_index].attributes = attributes;
			table->items[_ctx->item_index].num_attributes =
			    table->num_attributes;

			_ctx->parser_state = PARSER_STATE_ITEM_MAP;
			break;
		}
	case PARSER_STATE_ATTRIBUTE_KEY:{
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_MAP;
			break;
		}
	case PARSER_STATE_UNPROCESSED_KEY:{
			_ctx->parser_state = PARSER_STATE_UNPROCESSED_MAP;
			break;
		}
	default:{
			Warnx("batch_get_item_start_map - unexpected state: %d",
			     _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("batch_get_item_start_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int batch_get_item_map_key(void *ctx, const unsigned char *val,
				  unsigned int len)
{
	struct batch_get_item_ctx *_ctx = (struct batch_get_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("batch_get_item_map_key, val = %s, enter state %d", buf,
	      _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_RESPONSES, val, len)) {
				_ctx->parser_state = PARSER_STATE_RESPONSES_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_UNPROCESSED_KEYS, val, len)) {
				_ctx->parser_state = PARSER_STATE_UNPROCESSED_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("batch_get_item_map_key: Unknown root key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_RESPONSES_MAP:{
			/* Find the table index. */
			int table;

			for (table = 0; table < _ctx->num_tables; table++) {
				struct aws_dynamo_batch_get_item_response_table
				    *t;

				t = &(_ctx->tables[table]);
				if (len == t->name_len
				    && strncmp(val, t->name, len) == 0) {
					_ctx->table_index = table;
					break;
				}

			}
			if (table == _ctx->num_tables) {
				char table[len + 1];
				snprintf(table, len + 1, "%s", val);

				Warnx("batch_get_item_map_key: Unknown table '%s'.", table);
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_TABLE_KEY;

			/* We haven't processed any items yet. */
			_ctx->item_index = -1;
			break;
		}
	case PARSER_STATE_TABLE_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ITEMS, val, len)) {
				_ctx->parser_state = PARSER_STATE_ITEMS_KEY;
			} else
			    if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CONSUMED_CAPACITY, val, len)) {
				_ctx->parser_state = PARSER_STATE_CAPACITY_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("batch_get_item_map_key: Unknown table key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_ITEM_MAP:{
			/* Set the attribute index based on the name. */
			int attribute;
			struct aws_dynamo_batch_get_item_response_table *table;

			table = &(_ctx->tables[_ctx->table_index]);

			for (attribute = 0; attribute < table->num_attributes;
			     attribute++) {
				struct aws_dynamo_attribute *a =
				    &(table->attributes[attribute]);

				if (len == a->name_len
				    && strncmp(val, a->name, len) == 0) {
					_ctx->attribute_index = attribute;
					break;
				}
			}

			if (attribute == table->num_attributes) {
				char attr[len + 1];
				snprintf(attr, len + 1, "%s", val);

				Warnx("batch_get_item_map_key: Unknown attribute '%s'.", attr);
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_KEY;
			break;
		}
	case PARSER_STATE_ATTRIBUTE_MAP:{
			/* verify the attribute is of the expected type. */
			struct aws_dynamo_batch_get_item_response_table *table;
			struct aws_dynamo_attribute *a;
			const char *expected_type;

			table = &(_ctx->tables[_ctx->table_index]);

			a = &(table->attributes[_ctx->attribute_index]);
			expected_type = aws_dynamo_attribute_types[a->type];

			if (len != strlen(expected_type)
			    || strncmp(val, expected_type, len) != 0) {
				char type[len + 1];
				snprintf(type, len + 1, "%s", val);

				Warnx("batch_get_item_map_key: Unexpected type for attribute %s.  Got %s, expected %s.",
					a->name, type, expected_type);
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_ATTRIBUTE_VALUE;
			break;
		}
	default:{
			Warnx("batch_get_item_map_key - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("batch_get_item_map_key exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int batch_get_item_end_map(void *ctx)
{
	struct batch_get_item_ctx *_ctx = (struct batch_get_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("batch_get_item_end_map enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			_ctx->parser_state = PARSER_STATE_ITEM_MAP;
			break;
		}
	case PARSER_STATE_ITEM_MAP:{
			_ctx->parser_state = PARSER_STATE_ITEMS_ARRAY;
			break;
		}
	case PARSER_STATE_TABLE_MAP:{
			_ctx->parser_state = PARSER_STATE_RESPONSES_MAP;
			break;
		}
	case PARSER_STATE_RESPONSES_MAP:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_UNPROCESSED_MAP:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_ROOT_MAP:{
			_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
	default:{
			Warnx("batch_get_item_end_map - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("batch_get_item_end_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int batch_get_item_start_array(void *ctx)
{
	struct batch_get_item_ctx *_ctx = (struct batch_get_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("batch_get_item_start_array enter %d", _ctx->parser_state);
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
			Warnx("batch_get_item_start_array - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("batch_get_item_start_array exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int batch_get_item_end_array(void *ctx)
{
	struct batch_get_item_ctx *_ctx = (struct batch_get_item_ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("batch_get_item_end_array enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ITEMS_ARRAY:{
			_ctx->parser_state = PARSER_STATE_TABLE_MAP;
			break;
		}
	case PARSER_STATE_ATTRIBUTE_VALUE:{
			/* A String Set or a Number Set, no need for a state change. */
			break;
		}
	default:{
			Warnx("batch_get_item_end_array - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("batch_get_item_end_array exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static yajl_callbacks batch_get_item_callbacks = {
	.yajl_number = batch_get_item_number,
	.yajl_string = batch_get_item_string,
	.yajl_start_map = batch_get_item_start_map,
	.yajl_map_key = batch_get_item_map_key,
	.yajl_end_map = batch_get_item_end_map,
	.yajl_start_array = batch_get_item_start_array,
	.yajl_end_array = batch_get_item_end_array,
};

struct aws_dynamo_batch_get_item_response * aws_dynamo_parse_batch_get_item_response(const char *response, int response_len, struct aws_dynamo_batch_get_item_response_table
*tables, int num_tables)
{
	yajl_handle hand;
	yajl_status stat;
	struct batch_get_item_ctx _ctx = {
		.num_tables = num_tables,
		.tables = tables,
	};

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx("aws_dynamo_parse_batch_get_item_response: response alloc failed.");
		return NULL;
	}

	_ctx.r->tables = calloc(sizeof(*tables), num_tables);
	if (_ctx.r->tables == NULL) {
		Warnx("aws_dynamo_parse_batch_get_item_response: table alloc failed.");
		free(_ctx.r);
		return NULL;
	}

	_ctx.r->num_tables = num_tables;
	memcpy(_ctx.r->tables, tables, sizeof(*tables) * num_tables);

	hand = yajl_alloc(&batch_get_item_callbacks, NULL, NULL, &_ctx);

	yajl_parse(hand, response, response_len);

	stat = yajl_parse_complete(hand);

	if (stat != yajl_status_ok) {
		unsigned char *str =
		    yajl_get_error(hand, 1, response, response_len);
		Warnx("aws_dynamo_parse_batch_get_item_response: json parse failed, '%s'", (const char *)str);
		yajl_free_error(hand, str);
		yajl_free(hand);
		aws_dynamo_free_batch_get_item_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_dynamo_batch_get_item_response *aws_dynamo_batch_get_item(struct aws_handle *aws, const char *request, struct aws_dynamo_batch_get_item_response_table *tables, int
								     num_tables)
{
	const char *response;
	int response_len;
	struct aws_dynamo_batch_get_item_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_BATCH_GET_ITEM, request) == -1) {
		Warnx("aws_dynamo_batch_get_item: request failed.");
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_batch_get_item: Failed to get response.");
		return NULL;
	}

	if ((r = aws_dynamo_parse_batch_get_item_response(response, response_len,
						      tables, num_tables)) == NULL) {
		Warnx("aws_dynamo_batch_get_item: Failed to parse response.");
		return NULL;
	}

	return r;
}

void aws_dynamo_dump_batch_get_item_response(struct aws_dynamo_batch_get_item_response *r)
{
#ifdef DEBUG_PARSER
	int j;
	int i;
	struct aws_dynamo_batch_get_item_response_table *table;
	struct aws_dynamo_item *items;

	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	for (j = 0; j < r->num_tables; j++) {
		table = &(r->tables[j]);

		Debug("table %d (%s), consumed_capacity_units = %f, item count = %d",
		     j, table->name, table->consumed_capacity_units, table->num_items);

		items = table->items;
		for (i = 0; i < table->num_items; i++) {
			struct aws_dynamo_item *item = &(items[i]);
			Debug("Item %d, %d attributes", i, item->num_attributes);
			aws_dynamo_dump_attributes(item->attributes, item->num_attributes);
		}
	}
#endif
}

void aws_dynamo_free_batch_get_item_response(struct aws_dynamo_batch_get_item_response *r)
{
	int j;
	int i;
	struct aws_dynamo_batch_get_item_response_table *table;
	struct aws_dynamo_item *items;

	if (r == NULL) {
		return;
	}

	for (j = 0; j < r->num_tables; j++) {
		table = &(r->tables[j]);

		items = table->items;
		for (i = 0; i < table->num_items; i++) {
			struct aws_dynamo_item *item = &(items[i]);

			aws_dynamo_free_attributes(item->attributes, item->num_attributes);
		}
		free(table->items);
	}
	free(r->tables);
	free(r->unprocessed_keys);
	free(r);
}

