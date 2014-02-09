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

#ifndef _AWS_DYNAMO_BATCH_GET_ITEM_H_
#define _AWS_DYNAMO_BATCH_GET_ITEM_H_

#include "aws_dynamo.h"

struct aws_dynamo_batch_get_item_response_table {
	/* The name of the table. */
	const char *name;

	/* The length of the table name.  This is used by an optimization
	   to quickly determine a table name match. */
	int name_len;

	double consumed_capacity_units;

	/* The expected attributes for this table. */
	int num_attributes;
	struct aws_dynamo_attribute *attributes;

	/* The actual response data. */
	int num_items;
	struct aws_dynamo_item *items;
};

struct aws_dynamo_batch_get_item_response {
	int num_tables;
	struct aws_dynamo_batch_get_item_response_table *tables;
	char *unprocessed_keys;
};

struct aws_dynamo_batch_get_item_response *aws_dynamo_parse_batch_get_item_response(const unsigned char *response, int response_len, struct aws_dynamo_batch_get_item_response_table *tables, int num_tables);

struct aws_dynamo_batch_get_item_response *aws_dynamo_batch_get_item(struct aws_handle *aws, const char *request, struct aws_dynamo_batch_get_item_response_table *tables, int num_tables);

void aws_dynamo_free_batch_get_item_response(struct aws_dynamo_batch_get_item_response *r);

void aws_dynamo_dump_batch_get_item_response(struct aws_dynamo_batch_get_item_response *r);

#endif /* _AWS_DYNAMO_BATCH_GET_ITEM_H_ */

