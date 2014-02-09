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
#line 0 "aws_iam.c"
#include "aws_iam.c"
#line 3 "aws_iam_tests.c"

#include <assert.h>

static void test_parse_response(void) {
	const char *response = "{ \"Code\" : \"Success\", \"LastUpdated\" : \"2013-01-09T20:12:16Z\", \"Type\" : \"AWS-HMAC\", \"AccessKeyId\" : \"ASIAJGZKRYX3PFH23M5Q\", \"SecretAccessKey\" : \"PnF6486kWjFfTxt2YxlLiFUCFTgPgOGR0cuIMnwN\", \"Token\" : \"AQoDYXdzEE0agAJ4y0WmA2JV1B2RDfrWaD8HX3uyZR8opHuS2ynULVczFrLDF+Vx3iDDIHYE+IlJq8PwYCARno5VgMeYu/A8ZvYQnFz+CqCvcQqRY4PR+zi3z4EVA08xfxMvItcL/zuwFqDPLTPO729KmVaaJpI1ZfYLazc+giQdFxUgyb9XT3b6YTX6gDpuxrhuYtQD+wt54JEEELhSddQFB9mY9rwdzAuQcIMfBtzVJsmTCCLa7X2j0HRFID6X5Yc9yHwwHghFZwDVWLtD8nEPnvOCXpCKis0bNE2dKfpj33CLRJplSKBDyD0ZFym6vkved5AlRH5TcmYg1UdPdlRJdTL5+t/13IcyII+et4cF\", \"Expiration\" : \"2013-01-10T02:30:51Z\" }";
	char *id;
	char *key;
	time_t expiration;

	aws_iam_parse_credentials(response, strlen(response), &id, &key, &expiration);
	printf("%s %s %d", id, key, (int)expiration);
	free(id);
	free(key);
}

int main(int argc, char *argv[]) {
#ifdef DEBUG_ENABLED
	err_verbose = err_debug = 1;
#endif
	error_init("mydns", LOG_DAEMON);

	test_parse_response();

	return 0;
}
