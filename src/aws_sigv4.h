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

#ifndef _AWS_SIGV4_H_
#define _AWS_SIGV4_H_

#ifdef __cplusplus
extern "C" {
#endif

char *aws_sigv4_create_canonical_request(const char *http_request_method,
	const char *canonical_uri, const char *canonical_query_string,
	const char *canonical_headers, const char *signed_headers,
	const char *request_payload);

char *aws_sigv4_create_string_to_sign(time_t request_date,
	const char *region, const char *canonical_request);

char *aws_sigv4_create_signature(const char *aws_secret_access_key,
	const char *string_to_sign);

#endif /* _AWS_SIGV4_H_ */
