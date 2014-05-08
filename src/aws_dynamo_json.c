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

#include <string.h>
#include <stdlib.h>

#include "http.h"
#include "aws_sigv4.h"
#include "aws_dynamo_json.h"
#include "aws_dynamo_utils.h"
#include "jsmn.h"

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

const char *parser_state_string(int state)
{
	if (state < 0 || state > sizeof(parser_state_strings) / sizeof(parser_state_strings[0])) {
		return "invalid state";
	} else {
		return parser_state_strings[state];
	}
}

void dump_token(jsmntok_t * t, const char *response)
{
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
	default:
		type = "unknown";
		break;
	}
	str = strndup(response + t->start, t->end - t->start);
	Debug("%s start=%d end=%d size=%d -%s-", type, t->start, t->end, t->size, str);
	free(str);
}

