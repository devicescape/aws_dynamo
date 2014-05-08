/*
 * Copyright (c) 2013-2014 Devicescape Software, Inc.
 * This file is part of aws_dynamo, a C library for AWS.
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

#ifndef _AWS_KINESIS_PUT_RECORD_H_
#define _AWS_KINESIS_PUT_RECORD_H_

#include "aws_dynamo.h"

#ifdef  __cplusplus
extern "C" {
#endif

struct aws_kinesis_put_record_response {
    char *sequence_number;
    char *shard_id;
};

struct aws_kinesis_put_record_response *aws_kinesis_put_record(struct aws_handle *aws, const char *request);

void aws_kinesis_free_put_record_response(struct aws_kinesis_put_record_response *r);
void aws_kinesis_dump_put_record_response(struct aws_kinesis_put_record_response *r);

#ifdef  __cplusplus
}
#endif

#endif /* _AWS_KINESIS_PUT_RECORD_H_ */

