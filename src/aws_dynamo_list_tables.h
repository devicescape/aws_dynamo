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

#ifndef _AWS_DYNAMO_LIST_TABLES_H_
#define _AWS_DYNAMO_LIST_TABLES_H_

#include "aws_dynamo.h"

#ifdef __cplusplus
extern "C" {
#endif

struct aws_dynamo_list_tables_response {
	char *last_evaluated_table_name;
	int num_tables;
	char **table_names;
};

struct aws_dynamo_list_tables_response *
aws_dynamo_list_tables(struct aws_handle *aws, const char *request);

void aws_dynamo_free_list_tables_response(struct aws_dynamo_list_tables_response *r);

void aws_dynamo_dump_list_tables_response(struct aws_dynamo_list_tables_response *r);

#ifdef  __cplusplus
}
#endif

#endif /* _AWS_DYNAMO_LIST_TABLES_H_ */
