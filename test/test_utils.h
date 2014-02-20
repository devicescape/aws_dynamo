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

#ifndef _AWS_DYNAMO_TEST_UTILS_H_
#define _AWS_DYNAMO_TEST_UTILS_H_

#include "aws_dynamo.h"

#ifdef  __cplusplus
extern "C" {
#endif

int get_table_status(struct aws_handle *aws_dynamo, const char *table_name);
void wait_for_table(struct aws_handle *aws_dynamo, const char *table_name);
void create_test_table(struct aws_handle *aws_dynamo, const char *table_name,
		       const char *hash_key_type, const char *range_key_type);

#ifdef  __cplusplus
}
#endif

#endif /* _AWS_DYNAMO_TEST_UTILS_H_ */

