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

#include <assert.h>

static void test_aws_dynamo_parse_scan_response(const char *json,
	struct aws_dynamo_attribute *attributes, int num_attributes) {

	struct aws_dynamo_scan_response *r;


	r = aws_dynamo_parse_scan_response(json, strlen(json), attributes, num_attributes);

	aws_dynamo_dump_scan_response(r);
	aws_dynamo_free_scan_response(r);
}

#define NETWORK_IDX_TAG_XTYPE_TABLE_TAG_XTYPE	"tag_xtype"
#define NETWORK_IDX_TAG_XTYPE_TABLE_SSID			"ssid"

static void test_parse_scan_response_empty(void) {
	struct aws_dynamo_scan_response *r;
	struct aws_dynamo_attribute attributes[] = {
		{
			.type = AWS_DYNAMO_STRING,
			.name = NETWORK_IDX_TAG_XTYPE_TABLE_TAG_XTYPE,
			.name_len = strlen(NETWORK_IDX_TAG_XTYPE_TABLE_TAG_XTYPE),
		},
		{
			.type = AWS_DYNAMO_STRING,
			.name = NETWORK_IDX_TAG_XTYPE_TABLE_SSID,
			.name_len = strlen(NETWORK_IDX_TAG_XTYPE_TABLE_SSID),
		},
	};
	const char *json = "{\"ConsumedCapacityUnits\":109.0,\"Count\":0,\"Items\":[],\"LastEvaluatedKey\":{\"HashKeyElement\":{\"S\":\"FALLBACK_336e396b76366762656c\"}},\"ScannedCount\":14265}";

	r = aws_dynamo_parse_scan_response(json, strlen(json), attributes, sizeof(attributes) / sizeof(attributes[0]));

	assert(r != NULL);
	assert(r->count == 0);
	assert(r->scanned_count == 14265);
	assert(strcmp(r->hash_key->type, "S") == 0);
	assert(strcmp(r->hash_key->value, "FALLBACK_336e396b76366762656c") == 0);
	aws_dynamo_dump_scan_response(r);
	aws_dynamo_free_scan_response(r);
}

static void test_parse_scan_response_full(void) {
	struct aws_dynamo_attribute attributes[] = {
		{
			.type = AWS_DYNAMO_STRING,
			.name = NETWORK_IDX_TAG_XTYPE_TABLE_TAG_XTYPE,
			.name_len = strlen(NETWORK_IDX_TAG_XTYPE_TABLE_TAG_XTYPE),
		},
		{
			.type = AWS_DYNAMO_STRING,
			.name = NETWORK_IDX_TAG_XTYPE_TABLE_SSID,
			.name_len = strlen(NETWORK_IDX_TAG_XTYPE_TABLE_SSID),
		},
	};
	test_aws_dynamo_parse_scan_response("{ \"ConsumedCapacityUnits\": 1.5, \"Count\": 52, \"Items\": [ { \"tag_xtype\": { \"S\": \"thenorthface%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"petitauberge_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"americanairlinesadmiralsclub%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"attpark%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"thehomedepot%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"healthsouth%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"mcdonalds%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"fairfieldinn%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"seagardensbeachandtennisresort_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"seattlesbestcoffee%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"trufflescafe%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"razzoo%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"tulsainternational%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"starbucks%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"humanaguidancecenter%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"fedexoffice%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"brooksbrothers%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"applebees%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"doubletree%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"topshop%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"philadelphiainternational%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"rubytuesday%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"attexperiencestore%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"homewood%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"24hourfitness%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"sears%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"samsclub%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"southbaypavilion_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"barnesandnoble%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"attretail%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"thehoteladolphus_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"greyhound%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"limefresh%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"chz%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"pacificsunwear%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"saksfifthavenue%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"mvci%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"embassysuites%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"sunhealth%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"prometheus%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"wyndham%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"oneattplazawhitacretower_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"twoattplazaattdallasdatacenter_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"ihop%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"sfoamericanairlinesadmirals_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"attperformingartscenter_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"wellsfargo%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"attatlantaheadquarters%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"hilton%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"stanforduniversityathletics_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"hamptoninn%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } }, { \"tag_xtype\": { \"S\": \"washingtondulles%_2\" }, \"ssid\": { \"S\": \"61747477696669\" } } ], \"LastEvaluatedKey\": {\"HashKeyElement\":{\"S\":\"AttributeName1\"}, \"RangeKeyElement\":{\"N\":\"AttributeName2\"}}, \"ScannedCount\": 52  }",
	 	attributes, sizeof(attributes) / sizeof(attributes[0]));
}

static void test_parse_scan_response(void) {
	test_parse_scan_response_full();
	test_parse_scan_response_empty();
}

int main(int argc, char *argv[]) {

	test_parse_scan_response();

	return 0;
}
