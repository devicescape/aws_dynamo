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

static void dump_token(jsmntok_t *t, const char *response) {
	char *type;
	char *str;
	switch (t->type) {
	case JSMN_PRIMITIVE:
		type = "primitive";
		break;
	case JSMN_OBJECT:
		type = "object";
		break;
	case JSMN_ARRAY:
		type = "array";
		break;
	case JSMN_STRING:
		type = "string";
		break;
	}
	str = strndup(response + t->start, t->end - t->start);
	Debug("%s %d %d %d -%s-", type, t->start, t->end, t->size, str);
	free(str);
}
				
static int aws_dynamo_batch_write_item_handle_responses_key(jsmntok_t *tokens, int num_tokens,
	int start, char *response, 
	struct aws_dynamo_batch_write_item_consumed_capacity *r) {
	int i;
	int n;

	for (i = start; i < n; i++) {
		jsmntok_t *t;
		t = &(tokens[i]);
		dump_token(t, response);
	}
	return 10;
}

struct aws_dynamo_batch_write_item_response
*aws_dynamo_parse_batch_write_item_response(const char *response,
					    int response_len)
{
	struct aws_dynamo_batch_write_item_response *r;
	jsmn_parser parser;
	int num_tokens = 256;
	jsmntok_t tokens[num_tokens];
	int n;
	int i;
	int state = PARSER_STATE_NONE;

	jsmn_init(&parser);
	n = jsmn_parse(&parser, response, response_len, tokens,
		       sizeof(tokens) / sizeof(tokens[0]));

	r = calloc(sizeof(*r), 1);
	if (r == NULL) {
		Warnx("aws_dynamo_parse_batch_write_item_response: alloc failed.");
		return NULL;
	}

	for (i = 0; i < n; i++) {
		jsmntok_t *t;
		t = &(tokens[i]);
		Debug("%d", i);
		dump_token(t, response);
		//continue;

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
			if (t->type != JSMN_STRING) {
				Warnx("unexpected type, state %s", parser_state_string(state));
				goto failure;
			}
			if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_RESPONSES,
				response + t->start, t->end - t->start)) {
				state = PARSER_STATE_RESPONSES_KEY;
			} else if (AWS_DYNAMO_VALCMP(AWS_DYNAMO_JSON_UNPROCESSED_ITEMS,
				response + t->start, t->end - t->start)) {
				state = PARSER_STATE_UNPROCESSED_ITEMS_KEY;
			}
			break;
			}
		case PARSER_STATE_RESPONSES_KEY:{
			if (t->type != JSMN_OBJECT) {
				Warnx("unexpected type, state %s", parser_state_string(state));
				goto failure;
			}
			i = aws_dynamo_batch_write_item_handle_responses_key(tokens, num_tokens, i, response, &(r->responses));
			state = PARSER_STATE_ROOT_MAP;
			break;		
			}
		case PARSER_STATE_UNPROCESSED_ITEMS_KEY:{
			int end = t->end;
			int j;
			if (t->type != JSMN_OBJECT) {
				Warnx("unexpected type, state %s",
				      parser_state_string(state));
				goto failure;
			}
			if (r->unprocessed_items != NULL) {
				Warnx("duplicate unprocessed items");
				goto failure;
			}
			r->unprocessed_items =
			    strndup(response + t->start,
				    t->end - t->start);
			if (r->unprocessed_items == NULL) {
				Warnx("unprocessed items alloc failed");
				goto failure;
			}
			j = i;
			while (j < num_tokens && tokens[j].start < tokens[i].end) {
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
