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

#include <string.h>

#include "aws_dynamo.h"

struct aws_dynamo_batch_write_item_response
*aws_dynamo_parse_batch_write_item_response(const char *response,
                                            int response_len);

static void
test_aws_dynamo_parse_batch_write_item_response (const char *json)
{
  struct aws_dynamo_batch_write_item_response *r;

  r = aws_dynamo_parse_batch_write_item_response (json, strlen (json));

  aws_dynamo_dump_batch_write_item_response (r);
  aws_dynamo_free_batch_write_item_response (r);
}

static void
test_parse_batch_write_item_response_example (void)
{
  test_aws_dynamo_parse_batch_write_item_response ("{\"Responses\":{\"Thread\":{\"ConsumedCapacityUnits\":1.0},\"Reply\":{\"ConsumedCapacityUnits\":1.0}},\"UnprocessedItems\":{\"Reply\":[{\"DeleteRequest\":{\"Key\":{\"HashKeyElement\":{\"S\":\"Amazon DynamoDB#DynamoDB Thread 4\"},\"RangeKeyElement\":{\"S\":\"oops - accidental row\"}}}}]}}");
}

static void
test_parse_batch_write_item_response (void)
{
  test_parse_batch_write_item_response_example ();
}

int
main (int argc, char *argv[])
{
  test_parse_batch_write_item_response ();

  return 0;
}
