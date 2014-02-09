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

#define _GNU_SOURCE

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>

#include <yajl/yajl_parse.h>
#include <yajl/yajl_gen.h>

#include "aws.h"
#include "aws_dynamo.h"
#include "http.h"

#define ENV_ID_NAME	"AWS_ACCESS_KEY_ID"
#define ENV_KEY_NAME	"AWS_SECRET_ACCESS_KEY"

static struct aws_handle *aws;

void Errx(const char *fmt, ...)
{
	char msg[BUFSIZ];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stderr, "%s\n", msg);
}

void Err(const char *fmt, ...)
{
	char msg[BUFSIZ];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stderr, "%s\n", msg);
}

void Debug(const char *fmt, ...)
{
	char msg[BUFSIZ];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stderr, "%s\n", msg);
}

void Warnx(const char *fmt, ...)
{
	char msg[BUFSIZ];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stderr, "%s\n", msg);
}

static int reformat_null(void *ctx)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_null(g);
	return 1;
}

static int reformat_boolean(void *ctx, int boolean)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_bool(g, boolean);
	return 1;
}

static int reformat_number(void *ctx, const char *s, unsigned int l)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_number(g, s, l);
	return 1;
}

static int reformat_string(void *ctx, const unsigned char *stringVal,
			   unsigned int stringLen)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_string(g, stringVal, stringLen);
	return 1;
}

static int reformat_map_key(void *ctx, const unsigned char *stringVal,
			    unsigned int stringLen)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_string(g, stringVal, stringLen);
	return 1;
}

static int reformat_start_map(void *ctx)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_map_open(g);
	return 1;
}

static int reformat_end_map(void *ctx)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_map_close(g);
	return 1;
}

static int reformat_start_array(void *ctx)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_array_open(g);
	return 1;
}

static int reformat_end_array(void *ctx)
{
	yajl_gen g = (yajl_gen) ctx;
	yajl_gen_array_close(g);
	return 1;
}

static yajl_callbacks callbacks = {
	reformat_null,
	reformat_boolean,
	NULL,
	NULL,
	reformat_number,
	reformat_string,
	reformat_start_map,
	reformat_map_key,
	reformat_end_map,
	reformat_start_array,
	reformat_end_array
};

static int json_reformat(const char *json, int json_len,
			 unsigned char **out, unsigned int *outlen,
			 int beautify)
{
	int rv = -1;
	yajl_handle hand;
	yajl_gen_config conf = { 0, "  " };
	yajl_gen g;
	yajl_status stat;
	yajl_parser_config cfg = { 1, 1 };

	if (beautify) {
		conf.beautify = 1;
	}

	g = yajl_gen_alloc(&conf, NULL);

	hand = yajl_alloc(&callbacks, &cfg, NULL, (void *)g);

	stat = yajl_parse(hand, json, json_len);
	stat = yajl_parse_complete(hand);

	if (stat != yajl_status_ok && stat != yajl_status_insufficient_data) {
		unsigned char *str = yajl_get_error(hand, 1, json, json_len);
		fprintf(stderr, "%s", (const char *)str);
		yajl_free_error(hand, str);
		goto cleanup;
	} else {
		const unsigned char *temp;
		yajl_gen_get_buf(g, &temp, outlen);
		*out = strndup(temp, *outlen);
		if (*out == NULL) {
			fprintf(stderr, "allocation failed.");
			goto cleanup;
		}
		yajl_gen_clear(g);
	}

	rv = 0;
 cleanup:
	yajl_gen_free(g);
	yajl_free(hand);
	return rv;
}

void usage(char *argv[])
{
	printf("Usage: %s [OPTIONS]\n", argv[0]);
	printf("Supported options:\n");
	printf("\t-e	exit on any error\n");
	printf("\t-v	be verbose\n");
	printf(
"\n"
"This program reads requests to perform Amazon\n"
"DynamoDB operations from stdin and writes the\n"
"results to stdout.  The Amazon credentials used\n"
"are read from the environment variables: \n"
ENV_ID_NAME "     : your AWS ID\n"
ENV_KEY_NAME ": your AWS secret key\n"
"\n"
"Each request is of the form:\n"
"\n"
"request = <operation>  <json> ;<newline>\n"
"\n"
"<operation> is one of the 12 DynamoDB operations\n"
"(case-insensitive):\n"
"\n"
"    BatchGetItem\n"
"    BatchWriteItem\n"
"    CreateTable\n"
"    DeleteItem\n"
"    DeleteTable\n"
"    DescribeTable\n"
"    GetItem\n"
"    ListTables\n"
"    PutItem\n"
"    Query\n"
"    Scan\n"
"    UpdateItem\n"
"    UpdateTable\n"
"\n"
"<json> is the JSON associated with the request.\n"
"Every request is terminated by a semi-colon\n"
"followed by a newline.  For a full description of\n"
"the operations and the JSON arguments see here:\n"
"\n"
"http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/operationlist.html\n"
"\n"
"Example:\n"
"\n"
"$ cat example.dynamo\ndescribetable {\"TableName\":\"devel_device\"};\n"
"$ aws_dynamo_cmd < example.dynamo \n"
"{\n"
"  \"Table\": {\n"
"    \"CreationDateTime\": 1.331338422388E9,\n"
"    \"ItemCount\": 8,\n"
"    \"KeySchema\": {\n"
"      \"HashKeyElement\": {\n"
"        \"AttributeName\": \"uuid\",\n"
"        \"AttributeType\": \"S\"\n"
"      }\n"
"    },\n"
"    \"ProvisionedThroughput\": {\n"
"      \"ReadCapacityUnits\": 5,\n"
"      \"WriteCapacityUnits\": 5\n"
"    },\n"
"    \"TableName\": \"devel_device\",\n"
"    \"TableSizeBytes\": 2606,\n"
"    \"TableStatus\": \"ACTIVE\"\n"
"  }\n"
"}\n"
"$\n"
"\n");

}

enum {
	STATE_TARGET,
	STATE_BODY,
};

static char *get_target(const char *cmd)
{
	if (strcasecmp(cmd, "batchgetitem") == 0) {
		return AWS_DYNAMO_BATCH_GET_ITEM;
	} else if (strcasecmp(cmd, "batchwriteitem") == 0) {
		return AWS_DYNAMO_BATCH_WRITE_ITEM;
	} else if (strcasecmp(cmd, "createtable") == 0) {
		return AWS_DYNAMO_CREATE_TABLE;
	} else if (strcasecmp(cmd, "deleteitem") == 0) {
		return AWS_DYNAMO_DELETE_ITEM;
	} else if (strcasecmp(cmd, "deletetable") == 0) {
		return AWS_DYNAMO_DELETE_TABLE;
	} else if (strcasecmp(cmd, "describetable") == 0) {
		return AWS_DYNAMO_DESCRIBE_TABLE;
	} else if (strcasecmp(cmd, "getitem") == 0) {
		return AWS_DYNAMO_GET_ITEM;
	} else if (strcasecmp(cmd, "listtables") == 0) {
		return AWS_DYNAMO_LIST_TABLES;
	} else if (strcasecmp(cmd, "putitem") == 0) {
		return AWS_DYNAMO_PUT_ITEM;
	} else if (strcasecmp(cmd, "query") == 0) {
		return AWS_DYNAMO_QUERY;
	} else if (strcasecmp(cmd, "scan") == 0) {
		return AWS_DYNAMO_SCAN;
	} else if (strcasecmp(cmd, "updateitem") == 0) {
		return AWS_DYNAMO_UPDATE_ITEM;
	} else if (strcasecmp(cmd, "updatetable") == 0) {
		return AWS_DYNAMO_UPDATE_TABLE;
	} else {
		return NULL;
	}
}

int main(int argc, char *argv[])
{
	const char *aws_id = getenv(ENV_ID_NAME);
	const char *aws_key = getenv(ENV_KEY_NAME);
	char *line = NULL;
	size_t len;
	ssize_t n;
	int state = STATE_TARGET;
	const char *target = NULL;
	char *body = NULL;
	int exit_on_error = 0;
	int verbose = 0;
	int body_len = 0;

	for (;;) {
		int c;
		c = getopt(argc, argv, "ehv");
		if (c < 0)
			break;
		switch (c) {
		case 'e':
			exit_on_error = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'h':
		default:
			usage(argv);
			exit(1);
		}
	}

	if (aws_id == NULL) {
		fprintf(stderr,
			"AWS id not set, set " ENV_ID_NAME
			" in environment.\n");
		usage(argv);
		exit(1);
	}

	if (aws_key == NULL) {
		fprintf(stderr,
			"AWS key not set, set " ENV_KEY_NAME
			" in environment.\n");
		usage(argv);
		exit(1);
	}

	if ((aws = aws_init(aws_id, aws_key)) == NULL) {
		fprintf(stderr, "Initialization failed.\n");
		exit(1);
	}

	while ((n = getline(&line, &len, stdin)) != -1) {

		switch (state) {
		case STATE_TARGET:{
				char *ptr;
				char *start;

				/* Remove leading whitespace. */
				ptr = line;
				while (isspace(*ptr) && ((ptr - line) < n)) {
					ptr++;
				}

				/* collect consecutive non-space characters, this is the target. */
				start = ptr;
				while (!isspace(*ptr) && ((ptr - line) < n)) {
					ptr++;
				}
				*ptr = '\0';

				if (start == ptr) {
					continue;
				}

				target = get_target(start);
				if (target != NULL) {
					state = STATE_BODY;
					free(body);
					body = NULL;
					body_len = 0;
				} else {
					fprintf(stderr,
						"Invalid target, '%s'\n", line);
					if (exit_on_error) {
						exit(1);
					}
				}

				/* Advance past the nul terminator we wrote above. */
				ptr++;

				/* If anything else remains on the lines then assume it
				   is the start of the body. */
				if ((ptr - line) < n) {
					body = strndup(ptr, n - (ptr - line));
					if (body == NULL) {
						fprintf(stderr,
							"strndup failed\n");
						exit(1);
					}
					body_len = strlen(body);
				}
				break;
			}
		case STATE_BODY:{
				char *temp;

				temp = realloc(body, body_len + n + 1);
				if (temp == NULL) {
					fprintf(stderr, "allocation failed\n");
					exit(1);
				}
				body = temp;

				memcpy(body + body_len, line, n + 1);
				body_len = body_len + n;
				body[body_len] = '\0';
				break;
			}
		default:{
				fprintf(stderr, "Invalid state %d\n", state);
				exit(1);
				break;
			}
		}

		if (state != STATE_BODY || body == NULL) {
			continue;
		}

		/* Remove trailing whitespace. */
		while (isspace(body[body_len - 1]) && body > 0) {
			body_len--;
		}

		/* If the last character in the body is a semicolon then
		   we are done collecting the body.  Remove the semicolon
		   (since it isn't part of the json) and send the request. */

		if (body[body_len - 1] == ';') {
			unsigned char *request = NULL;
			int request_len;

			body_len--;


			if (json_reformat(body, body_len,
					  &request, &request_len, 0) == 0) {
				const char *response;
				int response_len;
				unsigned char *formatted_response;
				int formatted_response_len;

				if (verbose) {
					printf("%s:%s\n", target, request);
				}

				if (aws_dynamo_request(aws, target, request)
				    != 0) {
					fprintf(stderr, "request failed: '%s'\n", aws_dynamo_get_message(aws));
					fprintf(stderr, "%s, %s", target, body);
					if (exit_on_error) {
						exit(1);
					}
				}

				/* Don't print the response in the case of a successful put. */
				if (verbose == 1 || strcmp(target, AWS_DYNAMO_PUT_ITEM) != 0) {
					response = http_get_data(aws->http, &response_len);

					if (json_reformat
					    (response,
					     response_len,
					     &formatted_response,
				   	  &formatted_response_len, 1) == 0) {
						if (write(1,
						      formatted_response,
						      formatted_response_len) != formatted_response_len) {
							fprintf(stderr, "write failed: %s\n", strerror(errno));
						}
						free(formatted_response);
					}
				}
				state = STATE_TARGET;
				target = NULL;
			} else {
				fprintf(stderr, "json reformat failed\n");
				fprintf(stderr, "%s, %s", target, body);
				if (exit_on_error) {
					exit(1);
				}
			}
			free(request);
		}
	}

	free(body);
	free(line);

	return 0;
}
