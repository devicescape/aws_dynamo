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

#include "aws_dynamo_utils.h"
#include "http.h"
#include "aws_dynamo.h"
#include "aws_kinesis.h"
#include "aws_iam.h"
#include "aws_sigv4.h"
#include "aws.h"

#include <openssl/engine.h>
#include <openssl/sha.h>
#include <openssl/buffer.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/evp.h>

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>

#define AWS_DYNAMO_DEFAULT_MAX_RETRIES	10
#define AWS_DYNAMO_DEFAULT_HTTPS			1

struct aws_handle *aws_init(const char *aws_id, const char *aws_key) {
	struct aws_handle *aws = NULL;

	/* Initialize openssl. */
	ENGINE_load_builtin_engines();
	ENGINE_register_all_complete();

	aws = calloc(sizeof(*aws), 1);

	if (aws == NULL) {
		Errx("aws_init: Failed to allocate aws structure.");
		goto error;
	}

	aws->http = http_init();

	if (aws->http == NULL) {
		Errx("aws_init: Failed to initialize http handle.");
		goto error;
	}

	if (aws_id == NULL || aws_key == NULL) {
		/* Try to get credentials from environment. */
		aws_id = getenv("AWS_ACCESS_KEY_ID");
		aws_key = getenv("AWS_SECRET_ACCESS_KEY");
	}

	if (aws_id == NULL || aws_key == NULL) {
		/* Get token from EC2 Role. */
		aws->token = aws_iam_load_default_token(aws);
		if (aws->token == NULL) {
			Errx("aws_init: Failed to get aws token.");
			goto error;
		}
	} else {
		aws->aws_id = strdup(aws_id);

		if (aws->aws_id == NULL) {
			Errx("aws_init: Failed to copy id.");
			goto error;
		}

		aws->aws_key = strdup(aws_key);

		if (aws->aws_key == NULL) {
			Errx("aws_init: Failed to copy id.");
			goto error;
		}
	}

	aws->dynamo_max_retries = AWS_DYNAMO_DEFAULT_MAX_RETRIES;
	aws->dynamo_https = AWS_DYNAMO_DEFAULT_HTTPS;
	aws->dynamo_host = NULL;
	aws->dynamo_port = 0;
	aws->dynamo_region = NULL;

	return aws;

error:

	aws_deinit(aws);

	return NULL;
}


void aws_deinit(struct aws_handle *aws) {

	if (aws != NULL) {

		if (aws->http != NULL) {
			http_deinit(aws->http);
		}
		free(aws->aws_id);
		free(aws->aws_key);
		free(aws->dynamo_host);
		free(aws->dynamo_region);
		aws_free_session_token(aws->token);

		free(aws);
	}
}

char *aws_base64_encode(char *in, int in_len, size_t *out_len) {
    BIO *bio = NULL, *b64 = NULL;
    char *out = NULL;
    FILE *fp;

    fp = open_memstream(&out, out_len);

    if (fp == NULL) {
        goto error;
    }

    b64 = BIO_new(BIO_f_base64());
    if (b64 == NULL) {
        goto error;
    }
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bio = BIO_new_fp(fp, BIO_NOCLOSE);
    if (bio == NULL) {
        goto error;
    }
    bio = BIO_push(b64, bio);
    if (BIO_write(bio, in, in_len) <= 0) {
        goto error;
    }
    if (BIO_flush(bio) != 1) {
        goto error;
    }
    BIO_free_all(bio);
    fclose(fp);

    return out;
 error:
    Warnx("Failed to base64 encode data.");
    if (bio) {
        BIO_free(bio);
    }
    if (b64) {
        BIO_free(b64);
    }
    free(out);
    return NULL;
}

time_t aws_parse_iso8601_date(char *str) {
	struct tm t;
	time_t time;
	char *ptr;

	if ((ptr = strptime(str, "%Y-%m-%dT%H:%M:%S", &t)) == NULL) {
		Warnx("aws_parse_iso8601_date: Failed to parse date.");
		return -1;
	}

	/* Skip over decimal seconds. */
	if (*ptr == '.')
		ptr++;

	while (isdigit(*ptr)) {
		ptr++;
	}

	if (*ptr != 'Z') {
		Warnx("aws_parse_iso8601_date: Unexpected timezone.");
		return -1;
	}

	time = timegm(&t);

	if (time == -1) {
		Warnx("aws_parse_iso8601_date: timegm() failed.");
		return -1;
	}
	return time;
}

void aws_free_session_token(struct aws_session_token *token)
{
	if (token != NULL) {
		free(token->session_token);
		free(token->secret_access_key);
		free(token->access_key_id);
		free(token);
	}
}


static char *aws_dynamo_get_canonicalized_headers(struct http_headers *headers) {
    int i;
	int canonical_headers_len = 0;
	char *canonical_headers;
	char *ptr;

	/* Assume all header .name fields are lowercase. */
    
	/* Assume no duplicate headers (that is, all header names are
		distinct.) */

	/* Assume headers are already sorted alphabetically by header name. */

	for (i = 0; i < headers->count; i++) {
		canonical_headers_len += strlen(headers->entries[i].name);
		canonical_headers_len += strlen(headers->entries[i].value);
		canonical_headers_len += 2; /* ':' and '\n' */
	}
	canonical_headers_len++; /* \0 terminator */

	ptr = canonical_headers = calloc(sizeof(char), canonical_headers_len);

	if (ptr == NULL) {
		Err("aws_dynamo_get_canonicalized_headers: Failed to allocate.");
		return NULL;
	}

	for (i = 0; i < headers->count; i++) {
		int n;
		int remaining;

		remaining = canonical_headers_len - (ptr - canonical_headers);

		n = snprintf(ptr, remaining, "%s:%s\n", headers->entries[i].name,
			headers->entries[i].value);

		if (n == -1 || n >= remaining) {
			Warnx("aws_dynamo_get_canonicalized_headers: string buffer not large enough.");
			free(canonical_headers);
			return NULL;
		}
		ptr += n;
	}

	return canonical_headers;
}

int aws_post(struct aws_handle *aws, const char *aws_service, const char *target, const char *body) {
	char iso8601_basic_date[128];
	char token_header[4096];
	char host_header[256];
	char authorization[256];
	struct http_header hdrs[] = {
		/* Note: The .name fields must all lowercase and the headers included
			in the signature must be sorted here.  This simplifies the signature
			calculation. */
		{ .name = HTTP_HOST_HEADER, .value = host_header },
		{ .name = AWS_DYNAMO_DATE_HEADER, .value = iso8601_basic_date },
		{ .name = AWS_DYNAMO_TARGET_HEADER, .value = target },
		/* begin headers not included in signature. */
		{ .name = AWS_DYNAMO_AUTHORIZATION_HEADER, .value = authorization },
		{ .name = HTTP_CONTENT_TYPE_HEADER, .value = AWS_DYNAMO_CONTENT_TYPE },  /*Do not move this without changing content_type_offset*/
		{ .name = AWS_DYNAMO_TOKEN_HEADER, .value = token_header },
	};
    const int content_type_offset = 4;
	int total_num_headers;
	struct http_headers headers = {
		.count = 3,
		/* AWS_DYNAMO_AUTHORIZATION and HTTP_CONTENT_TYPE_HEADER are
		   not included for now since they are not used in the
		   signature calculation. */
		.entries = hdrs,
	};
	const char *signed_headers = HTTP_HOST_HEADER ";" AWS_DYNAMO_DATE_HEADER ";" AWS_DYNAMO_TARGET_HEADER;
	struct tm tm;
	time_t now;
	char *canonical_headers = NULL;
	char *hashed_canonical_request = NULL;
	char *string_to_sign = NULL;
	char *signature = NULL;
	int n;
	char *url = NULL;
	const char *scheme;
	const char *host;
	const char *region;
	const char *aws_secret_access_key;
	const char *aws_access_key_id;
	char yyyy_mm_dd[16];

    /* FIXME - choose host based on service enum, no strcmp. */
	if (strcmp(aws_service, "dynamodb") == 0) {
        /* FIXME: make https/http not DynamoDB specific, just have 1 
           boolean that controls for the whole library if we use https or not. */
        if (aws->dynamo_https) {
            scheme = "https";
        } else {
            scheme = "http";
        }
        if (aws->dynamo_host) {
            host = aws->dynamo_host;
        } else {
            host = AWS_DYNAMO_DEFAULT_HOST;
        }
        if (aws->dynamo_region) {
            region = aws->dynamo_region;
        } else {
            region = AWS_DYNAMO_DEFAULT_REGION;
        }
	} else if (strcmp(aws_service, "kinesis") == 0) {
		scheme = "https";  /*FIXME: This is only supported in HTTPS, ugh it will be slower....*/
        host = "kinesis.us-east-1.amazonaws.com";
        region = AWS_DYNAMO_DEFAULT_REGION; /* FIXME - make region configurable for Kinesis. */
        /* This is a bit of a hack, but fast.  Searching through the headers and doing O(N) strcmp is slow.*/
        /*Kinesis uses a different content-type*/
        hdrs[content_type_offset].value = AWS_KINESIS_CONTENT_TYPE;
	} else {
        Warnx("aws_post: Bad service");
        goto failure;
    }

	n = snprintf(host_header, sizeof(host_header), "%s", host);

	if (n == -1 || n >= sizeof(host_header)) {
		Warnx("aws_post: host header truncated");
		goto failure;
	}

	if (time(&now) == -1) {
		Warnx("aws_post: Failed to get time.");
		return -1;
	}

	if (aws->aws_id == NULL && aws->aws_key == NULL) {
		if (aws->token->expiration - now <= AWS_SESSION_REFRESH_TIME) {
			struct aws_session_token *new_token;

			new_token = aws_iam_load_default_token(aws);

			if (new_token == NULL) {
				Warnx("aws_post: Failed to refresh token.");
			} else {
				struct aws_session_token *old_token;
				old_token = aws->token;
				aws->token = new_token;
				aws_free_session_token(old_token);   
			}
		}

		n = snprintf(token_header, sizeof(token_header), "%s", aws->token->session_token);

		if (n == -1 || n >= sizeof(token_header)) {
			Warnx("aws_dynamo_post: token header truncated: %d", n);
			goto failure;
		}

		/* Include all headers, including the token header. */
		total_num_headers = sizeof(hdrs) / sizeof(hdrs[0]);

		aws_secret_access_key = aws->token->secret_access_key;
		aws_access_key_id = aws->token->access_key_id;
	} else {
		/* The -1 is to omit the token header, there is not token in this case. */
		total_num_headers = sizeof(hdrs) / sizeof(hdrs[0]) - 1;

		aws_secret_access_key = aws->aws_key;
		aws_access_key_id = aws->aws_id;
	}

	if (gmtime_r(&now, &tm) == NULL) {
		Warnx("aws_post: Failed to get time structure.");
		return -1;
	}

	if (strftime(iso8601_basic_date, sizeof(iso8601_basic_date),
		     "%Y%m%dT%H%M%SZ", &tm) == 0) {
		Warnx("aws_post: Failed to format time.");
		return -1;
	}

	if (strftime(yyyy_mm_dd, sizeof(yyyy_mm_dd), "%Y%m%d", &tm) == 0) {
		Warnx("aws_post: Failed to format date.");
		return -1;
	}

	canonical_headers = aws_dynamo_get_canonicalized_headers(&headers);

	if (canonical_headers == NULL) {
		Warnx("aws_post: Failed to get canonical_headers.");
		return -1;
	}

	hashed_canonical_request = aws_sigv4_create_hashed_canonical_request("POST", "/", "",
		canonical_headers, signed_headers, body);

	if (hashed_canonical_request == NULL) {
		Warnx("aws_post: Failed to get canonical request.");
		goto failure;
	}

	string_to_sign = aws_sigv4_create_string_to_sign(
		iso8601_basic_date, yyyy_mm_dd,
		region,
		aws_service, 
		hashed_canonical_request);
	if (string_to_sign == NULL) {
		Warnx("aws_post: Failed to get string to sign.");
		goto failure;
	}

	signature = aws_sigv4_create_signature(aws_secret_access_key,
		yyyy_mm_dd, 
		region,
		aws_service,
		string_to_sign);

	if (signature == NULL) {
		Warnx("aws_post: Failed to get signature.");
		goto failure;
	}

	n = snprintf(authorization, sizeof(authorization),
                 /* FIXME - hard coded region */
                 "AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request,SignedHeaders=%s,Signature=%s", 
                 aws_access_key_id, yyyy_mm_dd, region, aws_service, signed_headers, signature);

	if (n == -1 || n >= sizeof(authorization)) {
		Warnx("aws_post: authorization truncated");
		goto failure;
	}

	/* Include all headers now that the signature calculation is complete. */
	headers.count = total_num_headers;

#ifdef DEBUG_AWS_DYNAMO
	Debug("aws_post: '%s'", body);
#endif

        /* FIXME: make the kinesis service port configurable?  Or not? */
	if (aws->dynamo_port > 0) {
		if (asprintf(&url, "%s://%s:%d/", scheme, host, aws->dynamo_port) == -1) {
			Warnx("aws_post: failed to create url");
			goto failure;

		}
	} else {
		if (asprintf(&url, "%s://%s/", scheme, host) == -1) {
			Warnx("aws_post: failed to create url");
			goto failure;
		}
	}

	if (http_post(aws->http, url, body, &headers) != HTTP_OK) {
		Warnx("aws_post: HTTP post failed, will retry.");
		usleep(100000);
		if (http_post(aws->http, url, body, &headers) != HTTP_OK) {
			Warnx("aws_post: Retry failed.");
			goto failure;
		}
	}

#ifdef DEBUG_AWS_DYNAMO
	{
		int response_len;
		Debug("aws_post response: '%s'", http_get_data(aws->http, &response_len));
	}
#endif

	free(canonical_headers);
	free(hashed_canonical_request);
	free(string_to_sign);
	free(signature);
	free(url);

	return 0;
failure:
	free(canonical_headers);
	free(hashed_canonical_request);
	free(string_to_sign);
	free(signature);
	free(url);

	return -1;
}

