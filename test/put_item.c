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
#include "aws_dynamo.h"

#include <assert.h>

void test_put_item(struct aws_handle *aws_dynamo)
{

	char *put_request;
	struct aws_dynamo_put_item_response *put_r;

	/* Simple put that should succeed. */
	put_request = \
	"{\"TableName\":\"aws_dynamo_test_hash_N\",\
		\"Item\":{\
			\"hash\":{\"N\":\"1\"},\
			\"string\":{\"S\":\"stringValue\"},\
			\"binary\":{\"B\":\"dmFsdWU=\"}\
		}\
	}";
	put_r = aws_dynamo_put_item(aws_dynamo, put_request, NULL, 0);
	assert(put_r != NULL);
	assert(put_r->consumed_capacity_units == 1);
	assert(put_r->num_attributes == 0);
	assert(put_r->attributes == NULL);
	aws_dynamo_free_put_item_response(put_r);

	/* Put that should fail since the table isn't there. */
	put_request = \
	"{\"TableName\":\"aws_dynamo_test_table_not_present\",\
		\"Item\":{\
			\"hash\":{\"N\":\"1\"},\
			\"string\":{\"S\":\"stringValue\"},\
			\"binary\":{\"B\":\"dmFsdWU=\"}\
		}\
	}";
	put_r = aws_dynamo_put_item(aws_dynamo, put_request, NULL, 0);
	assert(put_r == NULL);

	/* Put that should fail since the item exists. */
	put_request = \
	"{\"TableName\":\"aws_dynamo_test_table_not_present\",\
		\"Item\":{\
			\"hash\":{\"N\":\"1\"},\
			\"string\":{\"S\":\"stringValue\"},\
			\"binary\":{\"B\":\"dmFsdWU=\"}\
		},\
		\"Expected\":{\"hash\":{\"Exists\":false}}\
	}";
	put_r = aws_dynamo_put_item(aws_dynamo, put_request, NULL, 0);
	assert(put_r == NULL);
}
int main(int argc, char *argv[]) {

	struct aws_handle *aws_dynamo;
	aws_dynamo = aws_init(NULL, NULL);
	assert(aws_dynamo != NULL);
	test_put_item(aws_dynamo);

	return 0;
}
