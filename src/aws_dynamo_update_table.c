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
#include "aws_dynamo_update_table.h"

#define DEBUG_PARSER 1

enum {
	PARSER_STATE_NONE,
	PARSER_STATE_ROOT_MAP,
	PARSER_STATE_TABLE_DESCRIPTION_KEY,
	PARSER_STATE_TABLE_DESCRIPTION_MAP,
	PARSER_STATE_CREATION_DATE_TIME_KEY,
	PARSER_STATE_KEY_SCHEMA_KEY,
	PARSER_STATE_KEY_SCHEMA_MAP,
	PARSER_STATE_HASH_KEY_ELEMENT_KEY,
	PARSER_STATE_HASH_KEY_ELEMENT_MAP,
	PARSER_STATE_HASH_KEY_ATTRIBUTE_NAME_KEY,
	PARSER_STATE_HASH_KEY_ATTRIBUTE_TYPE_KEY,
	PARSER_STATE_RANGE_KEY_ELEMENT_KEY,
	PARSER_STATE_RANGE_KEY_ELEMENT_MAP,
	PARSER_STATE_RANGE_KEY_ATTRIBUTE_NAME_KEY,
	PARSER_STATE_RANGE_KEY_ATTRIBUTE_TYPE_KEY,
	PARSER_STATE_PROVISIONED_THROUGHPUT_KEY,
	PARSER_STATE_PROVISIONED_THROUGHPUT_MAP,
	PARSER_STATE_READ_CAPACITY_UNITS_KEY,
	PARSER_STATE_WRITE_CAPACITY_UNITS_KEY,
	PARSER_STATE_LAST_INCREASE_DATE_TIME_KEY,
	PARSER_STATE_LAST_DECREASE_DATE_TIME_KEY,
	PARSER_STATE_TABLE_NAME_KEY,
	PARSER_STATE_ITEM_COUNT_KEY,
	PARSER_STATE_NUMBER_OF_DECREASES_TODAY_KEY,
	PARSER_STATE_TABLE_STATUS_KEY,
	PARSER_STATE_TABLE_SIZE_BYTES_KEY,
};

struct ctx {

	struct aws_dynamo_update_table_response *r;

	/* Index into the response structure. */
	int attribute_index;

	int parser_state;
};

static int handle_number(void *ctx, const char *val, unsigned int len)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("handle_number, val = %s, enter state %d", buf, _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_CREATION_DATE_TIME_KEY:{
			double date_time;
			if (aws_dynamo_json_get_double(val, len, &date_time) == -1) {
				Warnx("handle_number: failed to get creation date time.");
				return 0;
			}
			_ctx->r->creation_date_time = (int)date_time;

			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	case PARSER_STATE_READ_CAPACITY_UNITS_KEY:{
			if (aws_dynamo_json_get_int(val, len, &(_ctx->r->read_units)) == -1) {
				Warnx("handle_number: failed to get read units.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_PROVISIONED_THROUGHPUT_MAP;
			break;
		}
	case PARSER_STATE_WRITE_CAPACITY_UNITS_KEY:{
			if (aws_dynamo_json_get_int(val, len, &(_ctx->r->write_units)) == -1) {
				Warnx("handle_number: failed to get write units.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_PROVISIONED_THROUGHPUT_MAP;
			break;
		}
	case PARSER_STATE_LAST_INCREASE_DATE_TIME_KEY:{
			double date_time;
			if (aws_dynamo_json_get_double(val, len, &date_time) == -1) {
				Warnx("handle_number: failed to get last increase date time.");
				return 0;
			}
			_ctx->r->last_increase_date_time = (int)date_time;

			_ctx->parser_state = PARSER_STATE_PROVISIONED_THROUGHPUT_MAP;
			break;
		}
	case PARSER_STATE_LAST_DECREASE_DATE_TIME_KEY:{
			double date_time;
			if (aws_dynamo_json_get_double(val, len, &date_time) == -1) {
				Warnx("handle_number: failed to get last decrease date time.");
				return 0;
			}
			_ctx->r->last_decrease_date_time = (int)date_time;

			_ctx->parser_state = PARSER_STATE_PROVISIONED_THROUGHPUT_MAP;
			break;
		}
	case PARSER_STATE_NUMBER_OF_DECREASES_TODAY_KEY:{
			if (aws_dynamo_json_get_int(val, len, &(_ctx->r->number_decreases_today)) == -1) {
				Warnx("handle_number: failed to get number of decreases today.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_PROVISIONED_THROUGHPUT_MAP;
			break;
		}
	case PARSER_STATE_ITEM_COUNT_KEY:{
			if (aws_dynamo_json_get_int(val, len, &(_ctx->r->item_count)) == -1) {
				Warnx("handle_number: failed to get item count.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	case PARSER_STATE_TABLE_SIZE_BYTES_KEY:{
			if (aws_dynamo_json_get_int(val, len, &(_ctx->r->table_size_bytes)) == -1) {
				Warnx("handle_number: failed to get table size bytes.");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	default:{
			Warnx("handle_number - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_number exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_string(void *ctx, const unsigned char *val, unsigned int len)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("handle_string, val = %s, enter state %d", buf, _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_TABLE_STATUS_KEY:{
			if (aws_dynamo_json_get_table_status(val, len, &(_ctx->r->status))) {
				Warnx("handle_string - failed to get table status");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	case PARSER_STATE_TABLE_NAME_KEY:{
			_ctx->r->table_name = strndup(val, len);
			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	case PARSER_STATE_HASH_KEY_ATTRIBUTE_NAME_KEY:{
			_ctx->r->hash_key_name = strndup(val, len);
			_ctx->parser_state = PARSER_STATE_HASH_KEY_ELEMENT_MAP;
			break;
		}
	case PARSER_STATE_RANGE_KEY_ATTRIBUTE_NAME_KEY:{
			_ctx->r->range_key_name = strndup(val, len);
			_ctx->parser_state = PARSER_STATE_HASH_KEY_ELEMENT_MAP;
			break;
		}
	case PARSER_STATE_HASH_KEY_ATTRIBUTE_TYPE_KEY:{
			if (aws_dynamo_json_get_type(val, len, &(_ctx->r->hash_key_type))) {
				Warnx("handle_string - failed to get hash key type");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_HASH_KEY_ELEMENT_MAP;
			break;
		}
	case PARSER_STATE_RANGE_KEY_ATTRIBUTE_TYPE_KEY:{
			if (aws_dynamo_json_get_type(val, len, &(_ctx->r->range_key_type))) {
				Warnx("handle_string - failed to get hash key type");
				return 0;
			}
			_ctx->parser_state = PARSER_STATE_HASH_KEY_ELEMENT_MAP;
			break;
		}
	default:{
			Warnx("handle_string - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_string exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

/* one case entry for every "key":{ sets state to whatever map we are now in */
static int handle_start_map(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;

#ifdef DEBUG_PARSER
	Debug("handle_start_map, enter state %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_NONE:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_TABLE_DESCRIPTION_KEY:{
			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	case PARSER_STATE_KEY_SCHEMA_KEY:{
			_ctx->parser_state = PARSER_STATE_KEY_SCHEMA_MAP;
			break;
		}
	case PARSER_STATE_HASH_KEY_ELEMENT_KEY:{
			_ctx->parser_state = PARSER_STATE_HASH_KEY_ELEMENT_MAP;
			break;
		}
	case PARSER_STATE_RANGE_KEY_ELEMENT_KEY:{
			_ctx->parser_state = PARSER_STATE_RANGE_KEY_ELEMENT_MAP;
			break;
		}
	case PARSER_STATE_PROVISIONED_THROUGHPUT_KEY:{
			_ctx->parser_state = PARSER_STATE_PROVISIONED_THROUGHPUT_MAP;
			break;
		}
	default:{
			Warnx("handle_start_map - unexpected state: %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_start_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

/* each map is a case, compare key with all valid keys for that map
 * and set state accordingly. */
static int handle_map_key(void *ctx, const unsigned char *val, unsigned int len)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	char buf[len + 1];
	snprintf(buf, len + 1, "%s", val);

	Debug("handle_map_key, val = %s, enter state %d", buf, _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_TABLE_DESCRIPTION, val, len)) {
				_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("handle_map_key: Unknown root key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_TABLE_DESCRIPTION_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CREATION_DATE_TIME, val, len)) {
				_ctx->parser_state = PARSER_STATE_CREATION_DATE_TIME_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_KEY_SCHEMA, val, len)) {
				_ctx->parser_state = PARSER_STATE_KEY_SCHEMA_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_PROVISIONED_THROUGHPUT, val, len)) {
				_ctx->parser_state = PARSER_STATE_PROVISIONED_THROUGHPUT_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_TABLE_NAME, val, len)) {
				_ctx->parser_state = PARSER_STATE_TABLE_NAME_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_TABLE_STATUS, val, len)) {
				_ctx->parser_state = PARSER_STATE_TABLE_STATUS_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ITEM_COUNT, val, len)) {
				_ctx->parser_state = PARSER_STATE_ITEM_COUNT_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_TABLE_SIZE_BYTES, val, len)) {
				_ctx->parser_state = PARSER_STATE_TABLE_SIZE_BYTES_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("handle_map_key: Unknown table description key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_PROVISIONED_THROUGHPUT_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_READ_CAPACITY_UNITS, val, len)) {
				_ctx->parser_state = PARSER_STATE_READ_CAPACITY_UNITS_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_WRITE_CAPACITY_UNITS, val, len)) {
				_ctx->parser_state = PARSER_STATE_WRITE_CAPACITY_UNITS_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_LAST_INCREASE_DATE_TIME, val, len)) {
				_ctx->parser_state = PARSER_STATE_LAST_INCREASE_DATE_TIME_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_LAST_DECREASE_DATE_TIME, val, len)) {
				_ctx->parser_state = PARSER_STATE_LAST_DECREASE_DATE_TIME_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_NUMBER_OF_DECREASES_TODAY, val, len)) {
				_ctx->parser_state = PARSER_STATE_NUMBER_OF_DECREASES_TODAY_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("handle_map_key: Unknown provisioned throughput key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_KEY_SCHEMA_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_HASH_KEY_ELEMENT, val, len)) {
				_ctx->parser_state = PARSER_STATE_HASH_KEY_ELEMENT_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_RANGE_KEY_ELEMENT, val, len)) {
				_ctx->parser_state = PARSER_STATE_RANGE_KEY_ELEMENT_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("handle_map_key: Unknown key schema key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_HASH_KEY_ELEMENT_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ATTRIBUTE_NAME, val, len)) {
				_ctx->parser_state = PARSER_STATE_HASH_KEY_ATTRIBUTE_NAME_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ATTRIBUTE_TYPE, val, len)) {
				_ctx->parser_state = PARSER_STATE_HASH_KEY_ATTRIBUTE_TYPE_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("handle_map_key: Unknown hash key element key '%s'.", key);
				return 0;
			}
			break;
		}
	case PARSER_STATE_RANGE_KEY_ELEMENT_MAP:{
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ATTRIBUTE_NAME, val, len)) {
				_ctx->parser_state = PARSER_STATE_RANGE_KEY_ATTRIBUTE_NAME_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_ATTRIBUTE_TYPE, val, len)) {
				_ctx->parser_state = PARSER_STATE_RANGE_KEY_ATTRIBUTE_TYPE_KEY;
			} else {
				char key[len + 1];
				snprintf(key, len + 1, "%s", val);

				Warnx("handle_map_key: Unknown range key element key '%s'.", key);
				return 0;
			}
			break;
		}
	default:{
			Warnx("handle_map_key - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_map_key exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */
	return 1;
}

static int handle_end_map(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_end_map enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	case PARSER_STATE_ROOT_MAP:{
			_ctx->parser_state = PARSER_STATE_NONE;
			break;
		}
	case PARSER_STATE_TABLE_DESCRIPTION_MAP:{
			_ctx->parser_state = PARSER_STATE_ROOT_MAP;
			break;
		}
	case PARSER_STATE_PROVISIONED_THROUGHPUT_MAP:{
			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	case PARSER_STATE_HASH_KEY_ELEMENT_MAP:{
			_ctx->parser_state = PARSER_STATE_KEY_SCHEMA_MAP;
			break;
		}
	case PARSER_STATE_RANGE_KEY_ELEMENT_MAP:{
			_ctx->parser_state = PARSER_STATE_KEY_SCHEMA_MAP;
			break;
		}
	case PARSER_STATE_KEY_SCHEMA_MAP:{
			_ctx->parser_state = PARSER_STATE_TABLE_DESCRIPTION_MAP;
			break;
		}
	default:{
			Warnx("handle_end_map - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_end_map exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_start_array(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_start_array enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	default:{
			Warnx("handle_start_array - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_start_array exit %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	return 1;
}

static int handle_end_array(void *ctx)
{
	struct ctx *_ctx = (struct ctx *)ctx;
#ifdef DEBUG_PARSER
	Debug("handle_end_array enter %d", _ctx->parser_state);
#endif				/* DEBUG_PARSER */

	switch (_ctx->parser_state) {
	default:{
			Warnx("handle_end_array - unexpected state %d", _ctx->parser_state);
			return 0;
			break;
		}
	}

#ifdef DEBUG_PARSER
	Debug("handle_end_array exit %d", _ctx->parser_state);
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

struct aws_dynamo_update_table_response
*aws_dynamo_parse_update_table_response(const char *response, int response_len)
{
	yajl_handle hand;
	yajl_status stat;
	struct ctx _ctx = { 0 };

	_ctx.r = calloc(sizeof(*(_ctx.r)), 1);
	if (_ctx.r == NULL) {
		Warnx("aws_dynamo_parse_update_table_response: response alloc failed.");
		return NULL;
	}

	hand = yajl_alloc(&handle_callbacks, NULL, NULL, &_ctx);

	yajl_parse(hand, response, response_len);

	stat = yajl_parse_complete(hand);

	if (stat != yajl_status_ok) {
		unsigned char *str = yajl_get_error(hand, 1, response, response_len);
		Warnx("aws_dynamo_parse_update_table_response: json parse failed, '%s'", (const char *)str);
		yajl_free_error(hand, str);
		yajl_free(hand);
		aws_dynamo_free_update_table_response(_ctx.r);
		return NULL;
	}

	yajl_free(hand);
	return _ctx.r;
}

struct aws_dynamo_update_table_response *aws_dynamo_update_table(struct
								 aws_handle
								 *aws, const char
								 *request)
{
	const char *response;
	int response_len;
	struct aws_dynamo_update_table_response *r;

	if (aws_dynamo_request(aws, AWS_DYNAMO_UPDATE_TABLE, request) == -1) {
		return NULL;
	}

	response = http_get_data(aws->http, &response_len);

	if (response == NULL) {
		Warnx("aws_dynamo_update_table: Failed to get response.");
		return NULL;
	}

	if ((r = aws_dynamo_parse_update_table_response(response, response_len)) == NULL) {
		Warnx("aws_dynamo_update_table: Failed to parse response.");
		return NULL;
	}

	return r;
}

void aws_dynamo_dump_update_table_response(struct
					   aws_dynamo_update_table_response *r)
{
#ifdef DEBUG_PARSER
	if (r == NULL) {
		Debug("Empty response.");
		return;
	}

	Debug("table_name = %s", r->table_name);

	Debug("creation_date_time = %d", r->creation_date_time);
	Debug("last_increase_date_time = %d", r->last_increase_date_time);
	Debug("last_decrease_date_time = %d", r->last_decrease_date_time);
	Debug("hash_key_name = %s", r->hash_key_name);
	Debug("hash_key_type = %d", r->hash_key_type);
	if (r->range_key_name != NULL) {
		Debug("range_key_name = %s", r->range_key_name);
		Debug("range_key_type = %d", r->range_key_type);
	}
	Debug("write_units = %d", r->write_units);
	Debug("read_units = %d", r->read_units);

	Debug("status = %d", r->status);
#endif
}

void aws_dynamo_free_update_table_response(struct
					   aws_dynamo_update_table_response *r)
{
	if (r == NULL) {
		return;
	}

	free(r->hash_key_name);
	free(r->range_key_name);
	free(r->table_name);
	free(r);
}
