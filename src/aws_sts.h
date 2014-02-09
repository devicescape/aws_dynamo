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

#ifndef _AWS_STS_H_
#define _AWS_STS_H_

#include <time.h>
#include "aws.h"

#define AWS_STS_HOST "sts.amazonaws.com"

#define AWS_STS_SESSION_DURATION	"3600" /* seconds */

struct aws_session_token *aws_sts_get_session_token(struct aws_handle *aws,
	const char *aws_id, const char *key);


#endif /* _AWS_STS_H_ */
