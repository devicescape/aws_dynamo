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
#include <aws_dynamo/aws_dynamo.h>

#define USERNAME_ATTRIBUTE_NAME		"username"

#define REAL_NAME_ATTRIBUTE_NAME	"realName"
#define REAL_NAME_ATTRIBUTE_INDEX	0
#define LAST_SEEN_ATTRIBUTE_NAME	"lastSeen"
#define LAST_SEEN_ATTRIBUTE_INDEX	1

static int example_create_table(struct aws_handle *aws)
{
	const char *create_req = "{\
	\"TableName\":\"aws_dynamo_example_users\",\
	\"KeySchema\":{\
		\"HashKeyElement\":{\
			\"AttributeName\":\"" USERNAME_ATTRIBUTE_NAME "\",\
			\"AttributeType\":\"S\"\
		}},\
	\"ProvisionedThroughput\":{\
		\"ReadCapacityUnits\":1,\
		\"WriteCapacityUnits\":1\
	}\
}";
	const char *desc_req = "{\
	\"TableName\":\"aws_dynamo_example_users\"\
}";
	struct aws_dynamo_describe_table_response *desc_resp;
	struct aws_dynamo_create_table_response *create_resp;

	do {
		printf("Checking for example table.\n");

		/* Make sure the table exists and is active. */
		desc_resp = aws_dynamo_describe_table(aws, desc_req);
		if (desc_resp == NULL &&
		    aws_dynamo_get_errno(aws) == AWS_DYNAMO_CODE_RESOURCE_NOT_FOUND_EXCEPTION) {
			/* Create the table. */
			printf("Table not found, creating.\n");
			create_resp = aws_dynamo_create_table(aws, create_req);
			if (create_resp == NULL) {
				fprintf(stderr, "Failed to create table: %s\n",
					aws_dynamo_get_message(aws));
				return -1;
			}
			aws_dynamo_free_create_table_response(create_resp);
			printf("Waiting for table to be created.\n");
			sleep(20);
			continue;
		}

		if (desc_resp->status != AWS_DYNAMO_TABLE_STATUS_ACTIVE) {
			printf("Table is not active, waiting.\n");
			aws_dynamo_free_describe_table_response(desc_resp);
			sleep(20);
			continue;
		}
		aws_dynamo_free_describe_table_response(desc_resp);
		break;

	} while (1);
	return 0;
}

static int example_put_item(struct aws_handle *aws)
{
	return -1; /* TODO */
}

static int example_get_item(struct aws_handle *aws)
{

	struct aws_dynamo_attribute attributes[] = {
		{
		 /* Index: REAL_NAME_ATTRIBUTE_INDEX */
		 .name = REAL_NAME_ATTRIBUTE_NAME,
		 .name_len = strlen(REAL_NAME_ATTRIBUTE_NAME),
		 .type = AWS_DYNAMO_STRING,
		 },
		{
		 /* Index: LAST_SEEN_ATTRIBUTE_INDEX */
		 .name = LAST_SEEN_ATTRIBUTE_NAME,
		 .name_len = strlen(LAST_SEEN_ATTRIBUTE_NAME),
		 .type = AWS_DYNAMO_NUMBER,
		 .value.number.type = AWS_DYNAMO_NUMBER_INTEGER,
		 },
	};
	struct aws_dynamo_get_item_response *r = NULL;
	struct aws_dynamo_attribute *real_name;
	struct aws_dynamo_attribute *last_seen;

	const char *request = "{\
	\"TableName\":\"aws_dynamo_example_users\",\
	\"Key\":{\
		\"HashKeyElement\":{\"S\":\"jdoe\"}\
	},\
	\"AttributesToGet\":[\
		\"" REAL_NAME_ATTRIBUTE_NAME "\",\
		\"" LAST_SEEN_ATTRIBUTE_NAME "\"\
	]\
}";

	r = aws_dynamo_get_item(aws, request, attributes,
				sizeof(attributes) / sizeof(attributes[0]));

	if (r == NULL) {
		return -1;
	}

	if (r->item.attributes == NULL) {
		/* No item found. */
		aws_dynamo_free_get_item_response(r);
		return -1;
	}

	real_name = &(r->item.attributes[REAL_NAME_ATTRIBUTE_INDEX]);
	last_seen = &(r->item.attributes[LAST_SEEN_ATTRIBUTE_INDEX]);

	/* prints: "John Doe was last seen at 1391883778." */
	printf("%s was last seen at %llu\n", real_name->value.string,
	       *last_seen->value.number.value.integer_val);

	aws_dynamo_free_get_item_response(r);
	return 0;
}

int main(int argc, char *argv[])
{
	struct aws_handle *aws = aws_init(NULL, NULL);

	example_create_table(aws);
	example_put_item(aws);
	example_get_item(aws);

	aws_deinit(aws);
	return 0;
}
