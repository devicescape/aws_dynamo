/*
 * Copyright (c) 2006-2014 Devicescape Software, Inc.
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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "http.h"
#include "aws_dynamo_utils.h"

#define DEBUG_HTTP 1

/**
 * http_new_buffer - allocate a new buffer of the specified size
 * @size: size of the data buffer
 * Returns: Pointer to the newly allocated buffer, NULL on error
 */
static struct http_buffer *http_new_buffer(size_t size)
{
	struct http_buffer *buf;

	if ((buf = malloc(sizeof(*buf))) == NULL)
		return NULL;
	memset(buf, 0, sizeof(*buf));

	if ((buf->data = malloc(size)) == NULL) {
		free(buf);
		return NULL;
	}
	memset(buf->data, 0, size);

	buf->max = size;

	return buf;
}

/**
 * http_free_buffer - free a previously allocated buffer
 * @handle: HTTP handle
 * @buf: pointer to buffer
 */
static void http_free_buffer(struct http_buffer *buf)
{
	free(buf->data);
	free(buf);
}

/**
 * http_reset_buffer - reset the pointers in a buffer
 * @buf: buffer to reset
 */
static void http_reset_buffer(struct http_buffer *buf)
{
	buf->cur = 0;
}

#ifdef AWS_DYNAMO_HTTP_SIM

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

#include <sys/types.h>
#include <regex.h>

/**
 * http_strerror - obtain user readable error string
 * @error: error code to look up
 * Returns: string version of error
 */
const char *http_strerror(int error)
{
	static char buf[80];

	sprintf(buf, "%d", error);

	return buf;
}

/**
 * fetch_file - fetches a file into an HTTP buffer
 * @buf: the HTTP buffer to fetch into.
 * @filename: the name of the file to fetch.
 * Returns: 0 on success; -1 on error.
 */
int fetch_file(struct http_buffer *buf, const char *filename)
{
	int fd = -1;
	int ret = -1;

	if (buf == NULL || filename == NULL)
		goto error;

	/* Open the file */
	if ((fd = open(filename, O_RDONLY)) < 0) {
		Warnx("Failed to open file\n");
		goto error;
	}

	http_reset_buffer(buf);

	/* Read the file into the buffer */
	if ((buf->cur = read(fd, buf->data, buf->max - 1)) < 0) {
		Warnx("Error reading file\n");
		goto error;
	}

	/* Close the file */
	close(fd);
	fd = -1;
	ret = 0;
error:
	if (fd >= 0)
		close(fd);

	return ret;
}

/**
 * http_transaction - perform an HTTP GET or POST to a simulated URL
 * @handle: HTTP handle (unused).
 * @url: the request URL
 * @buf: the HTTP buffer to place the response in.
 * @redir: follow redirections automatically? (1=yes; 0=no)
 * Returns: 0 on success; -1 on failure.
 */
int http_transaction(void *handle, const char *initial_url,
		     struct http_buffer *buf, int redir,
		     struct http_headers *headers)
{
	char *filename = NULL, *effective_file = NULL, *alive_file = NULL;
	char *redir_file = NULL;
	int fd = -1;
	int ret = -1;
	char *url;
	struct stat statbuf;

	url = strdup(initial_url);

	if (url == NULL) {
		perror("strdup");
		return -1;
	}

	http_reset_buffer(buf);

	do {
		/* Alive checks use the PORTAL file if not alive, or are created
		   with alive_reply() when alive. */

		free(filename);
		if (strncmp(url, "POST-", 5) == 0)
			sim_get_best_match(url, &filename, 0);
		else
			sim_get_best_match(url, &filename, 1);

		if (! filename) {
			goto done;
		}

		fetch_file(buf, filename);

		/* Handle .redir files */

		redir_file = malloc(strlen(filename) + strlen(".redir") + 1);
		if (! redir_file)
			goto error;
		sprintf(redir_file, "%s.redir", filename);

		if ((fd = open(redir_file, O_RDONLY)) >= 0) {
			char *redir_url;

			if (stat(redir_file, &statbuf) != 0)
				goto error;

			redir_url = calloc(statbuf.st_size + 1, 1);
			if (! redir_url)
				goto error;
			if (read(fd, redir_url, statbuf.st_size) != statbuf.st_size) {
				perror("read");
				goto error;
			}
			close(fd);
			fd = -1;

			buf->redir = redir_url;

			if (redir == HTTP_FOLLOW) {
				free(url);
				url = http_make_full_url(handle, buf,
							 buf->redir, HTTP_NO_USE_BASE);
				http_reset_buffer(buf);

				Debug("Following HTTP redirect to: %s\n",
						url);
			}
		}

		free(redir_file);
		redir_file = NULL;

	} while (redir == HTTP_FOLLOW);

	effective_file = malloc(strlen(filename) + strlen(".effective") + 1);
	if (! effective_file)
		goto error;
	sprintf(effective_file, "%s.effective", filename);

	if ((buf->url = sim_read_file(effective_file)) != NULL) {
		Debug("Effective URL: %s\n", buf->url);
	} else {
		if (strncmp(url, "POST-", 5) == 0)
			buf->url = strdup(url + 5);
		else
			buf->url = strdup(url);
	}

	alive_file = malloc(strlen(filename) + strlen(".alive") + 1);
	if (! alive_file)
		goto error;
	sprintf(alive_file, "%s.alive", filename);

	if (stat(alive_file, &statbuf) == 0) {
		alive = 1;
	}

done:
	ret = 0;
error:

	if (fd >= 0)
		close(fd);
	free(filename);
	free(effective_file);
	free(alive_file);
	free(redir_file);
	free(url);

	return ret;
}


/**
 * http_get - fetch a URL into the specified buffer
 * @handle: HTTP handle
 * @url: URL to fetch
 * @buf: http buffer to store content in
 * @redir: follow redirections automatically? (1=yes; 0=no)
 * @ignore_cert: ignore certificate validation errors (1=yes; 0=no)
 * @con_close: close connection? (1=yes; 0=no)
 * @headers: a list of headers to be included in the request
 * Returns: result code
 */
int http_get(void *handle, const char *url,
		   int con_close,
		   struct http_headers *headers)
{
	int i;

	Debug("HTTP GET %s\n", url);

	if (headers) {
		for (i = 0; i < headers->count; i++) {
			Debug("HTTP HEADER %s: %s\n",
				  headers->entries[i].name,
				  headers->entries[i].value);
		}
	}
	return http_transaction(handle, url, buf, redir, headers);
}

/**
 * http_post - post a form back to the specified URL
 * @handle: HTTP handle
 * @buf: http_buffer structure pointer
 * @url: URL to post to
 * @data: data string to send with post
 * @redir: follow redirections automatically? (1=yes; 0=no)
 * @ignore_cert: ignore certificate validation errors (1=yes; 0=no)
 * @headers: a list of headers to be included in the request
 * Result: libcurl result code for post
 */
int http_post(void *handle, struct http_buffer *buf, const char *url,
		   const char *data, int redir, int ignore_cert,
		   struct http_headers *headers)
{
	char *url_post;
	char *post_data_file = NULL;
	struct stat statbuf;
	char *filename = NULL;
	int ret = -1;
	int i;

	Debug("HTTP POST %s\n", url);

	if (headers) {
		for (i = 0; i < headers->count; i++) {
			Debug("HTTP HEADER %s: %s\n",
				  headers->entries[i].name,
				  headers->entries[i].value);
		}
	}

	Debug("data: %s\n", data);

	url_post = malloc(strlen("POST-") + strlen(url) + 1);
	if (! url_post)
		return -1;
	sprintf(url_post, "POST-%s", url);

	sim_get_best_match(url_post, &filename, 1);

	if (filename == NULL)
		goto finished;

	post_data_file = malloc(strlen(filename) + strlen(".data") + 1);

	if (!post_data_file)
		goto finished;

	sprintf(post_data_file, "%s.data", filename);
	sim_url_escape(post_data_file);

	if (stat(post_data_file, &statbuf) == 0) {
		char *expected_data;

		Debug("Expected POST data from %s\n", post_data_file);

		expected_data = sim_read_file(post_data_file);

		if (!expected_data) {
			goto finished;
		}

		if (strcmp(data, expected_data)) {
			Debug("Unexpected POST data: '\n%s\n' expected: '\n%s\n'\n",
				  data, expected_data);
			http_reset_buffer(buf);
			free(expected_data);
			goto finished;
		}

		Debug("POST data in '%s' matched\n", post_data_file);

		free(expected_data);
	}

	ret = http_transaction(handle, url_post, buf, redir, headers);

finished:
	free(url_post);
	free(post_data_file);
	free(filename);

	return ret;
}

/**
 * http_init - initialise an HTTP session
 * @plaform: platform name
 * @uuid: device UUID
 * Returns: HTTP handle to use in other calls
 */
void *http_init(const char *platform, const char *uuid)
{
	return "ok";
}

/**
 * http_deinit - terminate an HTTP session
 * @handle: HTTP handle
 */
void http_deinit(void *handle)
{
}


#else

#include <curl/curl.h>

struct http_curl_handle {
       CURL *curl;
       struct http_buffer *buf;
       char agent[128];
};

/**
 * http_strerror - obtain user readable error string
 * @error: error code to look up
 * Returns: string version of error
 */
const char *http_strerror(int error)
{
	return curl_easy_strerror(error);
}

/**
 * _curl_easy_perform - call curl_easy_perform(), retry once
 * @curl: HTTP handle
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
static int _curl_easy_perform(CURL *curl)
{
	int ret;

	ret = curl_easy_perform(curl);

	if (ret == CURLE_OPERATION_TIMEOUTED) /* (sic) */ {
		ret = curl_easy_perform(curl);
	}

	if (ret == CURLE_OK)
		return HTTP_OK;

#if DEBUG_HTTP
	Debug("HTTP ERROR: %s\n", curl_easy_strerror(ret));
#endif

	if (ret == CURLE_SSL_PEER_CERTIFICATE ||
	    ret == CURLE_SSL_CERTPROBLEM ||
	    ret == CURLE_SSL_CACERT
#if LIBCURL_VERSION_NUM >= 0x071000 /* Only if this version supports it */
	    || ret == CURLE_SSL_CACERT_BADFILE
#endif
	   ) {
		return HTTP_CERT_FAILURE;
	}

	return HTTP_FAILURE;
}

/**
 * http_receive_data - callback for processing data received over HTTP
 * @ptr: pointer to the current chunk of data
 * @size: size of each element
 * @nmemb: number of elements
 * @arg: buffer to store chunk in
 * Returns: amount of data processed
 */
static size_t http_receive_data(void *ptr, size_t size, size_t nmemb, void *arg)
{
	size_t len = size * nmemb;
	struct http_buffer *buf = arg;

	if (len > (size_t)(buf->max - buf->cur)) {
		/* Too much data for buffer */
		len = buf->max - buf->cur;
		Warnx("Only storing %zd bytes; buffer too small\n", len);
	}
	memcpy(buf->data + buf->cur, ptr, len);
	buf->cur += len;

	return len;
}

/**
 * http_transaction - issue an HTTP transaction to the specified URL
 * @handle: HTTP handle
 * @url: URL to post to
 * @data: data string to send with post
 * @con_close: close connection? (1=yes; 0=no)
 * @hdrs: a linked list of headers to be included in the request
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
static int http_transaction(void *handle, const char *url,
			    const char *data, int con_close,
		   	    struct http_headers *hdrs)
{
	int ret;
	struct http_curl_handle *h = handle;
	CURL *curl = h->curl;
	struct curl_slist *headers = NULL;
	struct http_buffer *buf = h->buf;

	http_reset_buffer(buf);

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_receive_data);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, buf);

	if (data) {
		curl_easy_setopt(curl, CURLOPT_POST, 1);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
	} else {
		/* Use HTTP GET */
		curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
	}

	if (con_close != HTTP_NOCLOSE)
		curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1);
	else
		curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0);

	if (hdrs) {
		int i;

		for (i = 0; i < hdrs->count; i++) {
			char *header;

			if (asprintf(&header, "%s: %s",
				     hdrs->entries[i].name,
				     hdrs->entries[i].value) == -1) {
				if (headers)
					curl_slist_free_all(headers);
				return HTTP_FAILURE;
			}
			headers = curl_slist_append(headers, header);
			free(header);
		}
	}

	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	/* Perform the transfer */
	if ((ret = _curl_easy_perform(curl)) == 0) {
		/* Get a copy of the response code */
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &buf->response);
	}

	if (hdrs) {
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, NULL);
		curl_slist_free_all(headers);
	}

	if (buf->cur >= buf->max)
		buf->cur = buf->max - 1;
	buf->data[buf->cur] = '\0';

	return ret;
}

/**
 * http_get - fetch a URL into the specified buffer
 * @handle: HTTP handle
 * @url: URL to fetch
 * @con_close: close connection? (1=yes; 0=no)
 * @headers: a list of headers to be included in the request
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int http_get(void *handle, const char *url,
		   int con_close,
		   struct http_headers *headers)
{
	return http_transaction(handle, url, NULL,
				con_close, headers);
}

/**
 * http_init - initialise an HTTP session
 * Returns: HTTP handle to use in other calls
 */
void *http_init()
{
	struct http_curl_handle *h;
	CURL *curl;

	if ((h = malloc(sizeof(*h))) == NULL)
		return NULL;

   /* Create page buffer */
   h->buf = http_new_buffer(HTTP_PAGE_BUFFER_SIZE);
   if (h->buf == NULL) {
      Warnx("Failed to allocate buffer for page\n");
		free(h);
		return NULL;
   }

	/* Create the agent string */
	snprintf(h->agent, sizeof(h->agent), "Devicescape-AWS-DynamoDB-C/0.1");

	if ((curl = curl_easy_init()) == NULL) {
      Warnx("Failed to init curl easy handle\n");
		http_free_buffer(h->buf);
		free(h);
		return NULL;
	}

	/* Set the maximum time in seconds that a transfer can take. */
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10);

	/* Tell curl not to use signals for timeout's etc. */
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
	curl_easy_setopt(curl, CURLOPT_USERAGENT, h->agent);

	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2);

	h->curl = curl;

	return h;
}

/**
 * http_deinit - terminate an HTTP session
 * @handle: HTTP handle
 */
void http_deinit(void *handle)
{
	struct http_curl_handle *h = handle;

	curl_easy_cleanup(h->curl);
	http_free_buffer(h->buf);
	free(h);
}

/**
 * http_get_data - return pointer to raw data buffer (read-only)
 * @handle: HTTP library handle
 * @buf: pointer to buffer structure
 * @len: length of buffer (out)
 * Returns: const pointer to raw data
 */
const unsigned char *http_get_data(void *handle, int *len)
{
	struct http_curl_handle *h = handle;
	struct http_buffer *buf = h->buf;

	if (len) {
		*len = buf->cur;
		return buf->data;
	}
	return NULL;
}

int http_get_response_code(void *handle)
{
	struct http_curl_handle *h = handle;
	struct http_buffer *buf = h->buf;

	return buf->response;
}

/**
 * http_post - post a form back to the specified URL, internal function
 * @handle: HTTP handle
 * @url: URL to post to
 * @data: data string to send with post
 * @headers: a list of headers to be included in the request
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int http_post(void *handle, const char *url,
		    const char *data,
		   struct http_headers *hdrs)
{
	struct http_curl_handle *h = handle;
	struct http_buffer *buf = h->buf;
	int rv;
#if DEBUG_HTTP
	int i;

	Debug("HTTP POST: %s", url);

	if (hdrs) {
		for (i = 0 ; i < hdrs->count; i++) {
			Debug("HTTP HEADER: %s: %s", hdrs->entries[i].name, hdrs->entries[i].value);
		}
	}
#endif

	rv = http_transaction(handle, url, data, HTTP_NOCLOSE, hdrs);

#if DEBUG_HTTP
	Debug("HTTP RECV %d BYTES:\n%s\n", buf->cur, buf->data);

	if (rv == HTTP_CERT_FAILURE)
		Debug("HTTP ERROR: certificate problem\n");
#endif

	return rv;
}

/**
 * http_url_encode - URL encode a string
 * @str: NULL-terminated string to encode
 * Returns: allocated string URL encoded for AWS
 */
char *http_url_encode(void *handle, const char *string)
{
	struct http_curl_handle *h = handle;
	char *curl_encoded;
	char *encoded;

	curl_encoded = curl_easy_escape(h->curl, string, strlen(string));
	if (curl_encoded == NULL) {
		return NULL;
	}

	encoded = strdup(curl_encoded);
	curl_free(curl_encoded);
	return encoded;
}

int http_set_https_certificate_file(void *handle, const char *filename)
{
	struct http_curl_handle *h = handle;

	curl_easy_setopt(h->curl, CURLOPT_CAINFO, filename);
	return 0;
}

#endif /* AWS_DYNAMO_HTTP_SIM */
