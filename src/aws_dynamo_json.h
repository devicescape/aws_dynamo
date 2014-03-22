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

#ifndef _AWS_DYNAMO_JSON_H_
#define _AWS_DYNAMO_JSON_H_

#include "aws_dynamo.h"
#include "jsmn.h"

#ifdef __cplusplus
extern "C" {
#endif

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

const char *parser_state_string(int state);
void dump_token(jsmntok_t * t, const char *response);

#ifdef  __cplusplus
}
#endif

#endif /* _AWS_DYNAMO_JSON_H_ */
