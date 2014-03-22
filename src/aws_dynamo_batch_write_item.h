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

#ifndef _AWS_DYNAMO_BATCH_WRITE_ITEM_H_
#define _AWS_DYNAMO_BATCH_WRITE_ITEM_H_

#include "aws_dynamo.h"

#ifdef __cplusplus
extern "C" {
#endif

struct aws_dynamo_batch_write_item_consumed_capacity {
	double consumed_capacity_units;
	char *table_name;
};

struct aws_dynamo_batch_write_item_response {
	int num_responses;
	struct aws_dynamo_batch_write_item_consumed_capacity *responses;
	char *unprocessed_items;
};

struct aws_dynamo_batch_write_item_response *
aws_dynamo_batch_write_item(struct aws_handle *aws, const char *request);

void aws_dynamo_free_batch_write_item_response(struct aws_dynamo_batch_write_item_response *r);

void aws_dynamo_dump_batch_write_item_response(struct aws_dynamo_batch_write_item_response *r);

#ifdef  __cplusplus
}
#endif

#endif /* _AWS_DYNAMO_BATCH_WRITE_ITEM_H_ */
