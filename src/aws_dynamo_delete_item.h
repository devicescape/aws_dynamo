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

#ifndef _AWS_DYNAMO_DELETE_ITEM_H_
#define _AWS_DYNAMO_DELETE_ITEM_H_

#include "aws_dynamo.h"

#ifdef __cplusplus
extern "C" {
#endif

struct aws_dynamo_delete_item_response {
	double consumed_capacity_units;
		
	struct aws_dynamo_item item;
};

struct aws_dynamo_delete_item_response *aws_dynamo_parse_delete_item_response(
	const char *response,
	int response_len, struct aws_dynamo_attribute *attributes, int num_attributes);

struct aws_dynamo_delete_item_response *aws_dynamo_delete_item(struct aws_handle *aws,
	const char *request, struct aws_dynamo_attribute *attributes, int num_attributes);

void aws_dynamo_free_delete_item_response(struct aws_dynamo_delete_item_response *r);

void aws_dynamo_dump_delete_item_response(struct aws_dynamo_delete_item_response *r);

#ifdef  __cplusplus
}
#endif

#endif /* _AWS_DYNAMO_DELETE_ITEM_H_ */
