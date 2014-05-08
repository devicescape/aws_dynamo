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
#include "jsmn.h"

#include <stdlib.h>

#include "http.h"
#include "aws_dynamo.h"
#include "aws_dynamo_json.h"

static int aws_dynamo_handle_responses_key(jsmntok_t * tokens,
					   int num_tokens, int start_index,
					   const char *response, struct
					   aws_dynamo_batch_write_item_response
					   *r)
{
	int j;
	int state = PARSER_STATE_NONE;

	j = start_index;
	while (j < num_tokens && tokens[j].start < tokens[start_index].end) {
		jsmntok_t *t;
		t = &(tokens[j]);

		switch (state) {
		case PARSER_STATE_NONE:{
				if (t->type != JSMN_OBJECT) {
					Warnx("unexpected type, state %s",
					      parser_state_string(state));
					goto failure;
				}
				state = PARSER_STATE_ROOT_MAP;
				break;
			}
		case PARSER_STATE_ROOT_MAP:{
				char *table_name;
				struct aws_dynamo_batch_write_item_consumed_capacity *new_responses;
				if (t->type != JSMN_STRING) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				table_name = strndup(response + t->start, t->end - t->start);
				if (table_name == NULL) {
					Warnx("failed to allocate table name");
					goto failure;
				}
				new_responses = realloc(r->responses, sizeof(r->responses[0]) * (r->num_responses + 1));
				if (new_responses == NULL) {
					Warnx("failed to allocate responses");
					free(table_name);
					goto failure;
				}
				r->responses = new_responses;
				r->responses[r->num_responses].table_name = table_name;
				r->num_responses++;
				state = PARSER_STATE_TABLE_NAME_KEY;
				break;
			}
		case PARSER_STATE_TABLE_NAME_KEY:{
				if (t->type != JSMN_OBJECT) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				state = PARSER_STATE_TABLE_NAME_MAP;
				break;
			}
		case PARSER_STATE_TABLE_NAME_MAP:{
				if (t->type != JSMN_STRING) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				if (!AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_CONSUMED_CAPACITY, response + t->start, t->end - t->start)) {
					Warnx("unexpected string value, state %s", parser_state_string(state));
					goto failure;
				}
				state = PARSER_STATE_CONSUMED_CAPACITY_KEY;
				break;
			}
		case PARSER_STATE_CONSUMED_CAPACITY_KEY:{
				if (t->type != JSMN_PRIMITIVE) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				aws_dynamo_json_get_double(response + t->start, t->end - t->start, &(r->responses[r->num_responses - 1].consumed_capacity_units));
				state = PARSER_STATE_ROOT_MAP;
				break;
			}
		}
		j++;
	}
	return j - 1;
 failure:
	return -1;
}

struct aws_dynamo_batch_write_item_response *
aws_dynamo_parse_batch_write_item_response(const char *response, int response_len)
{
	struct aws_dynamo_batch_write_item_response *r;
	jsmn_parser parser;
	int num_tokens = 256;
	jsmntok_t tokens[num_tokens];
	int n;
	int i;
	int state = PARSER_STATE_NONE;

	r = calloc(sizeof(*r), 1);
	if (r == NULL) {
		Warnx("aws_dynamo_parse_batch_write_item_response: alloc failed.");
		return NULL;
	}

	jsmn_init(&parser);
	/* TODO: check return value of jsm_parse. */
	n = jsmn_parse(&parser, response, response_len, tokens,
		       sizeof(tokens) / sizeof(tokens[0]));

	for (i = 0; i < n; i++) {
		jsmntok_t *t;
		t = &(tokens[i]);

		switch (state) {
		case PARSER_STATE_NONE:{
				if (t->type != JSMN_OBJECT) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				state = PARSER_STATE_ROOT_MAP;
				break;
			}
		case PARSER_STATE_ROOT_MAP:{
				if (t->type != JSMN_STRING) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_RESPONSES, response + t->start, t->end - t->start)) {
					state = PARSER_STATE_RESPONSES_KEY;
				} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_UNPROCESSED_ITEMS, response + t->start, t->end - t->start)) {
					state = PARSER_STATE_UNPROCESSED_ITEMS_KEY;
				}
				break;
			}
		case PARSER_STATE_RESPONSES_KEY:{
				if (t->type != JSMN_OBJECT) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				i = aws_dynamo_handle_responses_key(tokens, n, i, response, r);
				if (i == -1) {
					Warnx("Failed to parse responses key");
					goto failure;
				}
				state = PARSER_STATE_ROOT_MAP;
				break;
			}
		case PARSER_STATE_UNPROCESSED_ITEMS_KEY:{
				int j;
				if (t->type != JSMN_OBJECT) {
					Warnx("unexpected type, state %s", parser_state_string(state));
					goto failure;
				}
				if (r->unprocessed_items != NULL) {
					Warnx("duplicate unprocessed items");
					goto failure;
				}
				r->unprocessed_items = strndup(response + t->start, t->end - t->start);
				if (r->unprocessed_items == NULL) {
					Warnx("unprocessed items alloc failed");
					goto failure;
				}
				j = i;
				while (j < num_tokens && j < n && tokens[j].start < tokens[i].end) {
					j++;
				}
				i = j - 1;
				break;

			}
		}
	}
	aws_dynamo_dump_batch_write_item_response(r);

	return r;
 failure:
	free(r);
	return NULL;
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

	if ((r = aws_dynamo_parse_batch_write_item_response(response, response_len)) == NULL) {
		Warnx("aws_dynamo_batch_write_item: Failed to parse response.");
		return NULL;
	}

	return r;
}

void aws_dynamo_dump_batch_write_item_response(struct aws_dynamo_batch_write_item_response *r)
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

void aws_dynamo_free_batch_write_item_response(struct aws_dynamo_batch_write_item_response *r)
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
