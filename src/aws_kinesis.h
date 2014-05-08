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

#ifndef _AWS_KINESIS_H_
#define _AWS_KINESIS_H_

#include "aws.h"

#ifdef  __cplusplus
extern "C" {
#endif

#define AWS_KINESIS_DEFAULT_HOST "kinesis.us-east-1.amazonaws.com"
#define AWS_KINESIS_PUT_RECORD "Kinesis_20131202.PutRecord"

enum {
    /*Do not change*/
    AWS_KINESIS_CODE_UNKNOWN=0,
    AWS_KINESIS_CODE_NONE,
    AWS_KINESIS_CODE_INCOMPLETE_SIGNATURE,
    AWS_KINESIS_CODE_INTERNAL_FAILURE,
    AWS_KINESIS_CODE_INVALID_ACTION,
    AWS_KINESIS_CODE_INVALID_CLIENTTOKENID,
    AWS_KINESIS_CODE_INVALID_PARAMETER_COMBINATION,
    AWS_KINESIS_CODE_INVALID_PARAMETER_VALUE,
    AWS_KINESIS_CODE_INVALID_QUERY_PARAMETER,
    AWS_KINESIS_CODE_MALFORMED_QUERY_STRING,
    AWS_KINESIS_CODE_MISSING_ACTION,
    AWS_KINESIS_CODE_MISSING_AUTHENTICATION_TOKEN,
    AWS_KINESIS_CODE_MISSING_PARAMETER,
    AWS_KINESIS_CODE_OPT_IN_REQUIRED,
    AWS_KINESIS_CODE_REQUEST_EXPIRED,
    AWS_KINESIS_CODE_SERVICE_UNAVAILABLE,
    AWS_KINESIS_CODE_THROTTLING,
    AWS_KINESIS_CODE_VALIDATION_ERROR,
    AWS_KINESIS_CODE_SERIALIZATION_ERROR,
    /*Add below here*/
    /*Add above here*/
    AWS_KINESIS_CODE_SIZE,   
};

#define AWS_KINESIS_JSON_SEQUENCE_NUMBER "SequenceNumber"
#define AWS_KINESIS_JSON_SHARD_ID "ShardId"

/* Content-Type: value for Kinesis requests */
#define AWS_KINESIS_CONTENT_TYPE	"application/x-amz-json-1.1"

/**
 * aws_error - Generic AWS Responses
 */
struct aws_errors {
    int code;
    const char *error;
    const char *reason;
    int http_code;
};

int aws_kinesis_request(struct aws_handle *aws, const char *target, const char *body);

#ifdef  __cplusplus
}
#endif

#include "aws_kinesis_put_record.h"

#endif /* _AWS_KINESIS_H_ */
