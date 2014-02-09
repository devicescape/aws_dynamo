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

#ifndef _AWS_DYNAMO_DESCRIBE_TABLE_H_
#define _AWS_DYNAMO_DESCRIBE_TABLE_H_

#include "aws_dynamo.h"

struct aws_dynamo_describe_table_response {
	int creation_date_time;
	long int item_count;

	long int write_units;
	long int read_units;

	int last_increase_date_time;
	int last_decrease_date_time;
	const char *hash_key_name;
	enum aws_dynamo_attribute_type hash_key_type;
	
	const char *range_key_name;
	enum aws_dynamo_attribute_type range_key_type;

	const char *table_name;
	long int table_size;


	enum aws_dynamo_table_status status;
};

struct aws_dynamo_describe_table_response *aws_dynamo_describe_table(struct aws_handle *aws, const char *request);

void aws_dynamo_free_describe_table_response(struct aws_dynamo_describe_table_response *r);
void aws_dynamo_dump_describe_table_response(struct aws_dynamo_describe_table_response *r);

#endif /* _AWS_DYNAMO_DESCRIBE_TABLE_H_ */

