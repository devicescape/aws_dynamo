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

#include <string.h>
#include <unistd.h>
#include <assert.h>

#include "aws_dynamo.h"
#include "test_utils.h"

static void setup(void)
{
	struct aws_handle *aws_dynamo = aws_init(NULL, NULL);
	create_test_table(aws_dynamo, "aws_dynamo_test_hash_N", "N", NULL);
	aws_deinit(aws_dynamo);
}

int main(int argc, char *argv[])
{
	setup();
	return 0;
}
