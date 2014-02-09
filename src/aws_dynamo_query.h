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

#ifndef _AWS_DYNAMO_QUERY_H_
#define _AWS_DYNAMO_QUERY_H_

#include "aws_dynamo.h"

struct aws_dynamo_query_response {
	double consumed_capacity_units;
	int count;
		
	struct aws_dynamo_item *items;

	/* Last evaluated keys. */
	struct aws_dynamo_key *hash_key;
	struct aws_dynamo_key *range_key;
};

struct aws_dynamo_query_response *aws_dynamo_parse_query_response(const char *response,
	int response_len, struct aws_dynamo_attribute *attributes, int num_attributes);

struct aws_dynamo_query_response *aws_dynamo_query(struct aws_handle *aws,
	const char *request, struct aws_dynamo_attribute *attributes, int num_attributes);

struct aws_dynamo_query_response *aws_dynamo_query_combine_and_free_responses(
					 struct aws_dynamo_query_response *current,
					 struct aws_dynamo_query_response *next);

void aws_dynamo_free_query_response(struct aws_dynamo_query_response *r);

void aws_dynamo_dump_query_response(struct aws_dynamo_query_response *r);

#endif /* _AWS_DYNAMO_QUERY_H_ */
