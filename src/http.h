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

#ifndef _HTTP_H_
#define _HTTP_H_

#define HTTP_PAGE_BUFFER_SIZE	1048576

/* Close connection or not */
#define HTTP_NOCLOSE	0
#define HTTP_CLOSE	1

/* Allow cert errors or not */
#define HTTP_NO_CERT_ERRORS	0
#define HTTP_IGNORE_CERT_ERRORS	1

/* Whether or not to honor the HTML base tag when making full URLs. */
#define HTTP_NO_USE_BASE	0
#define HTTP_USE_BASE	1

/* http_alive_check return values */
#define HTTP_DEAD_NONET		-2 
#define HTTP_DEAD		-1 
#define HTTP_ALIVE		0 
#define HTTP_ALIVE_NOTIFY	1 

/* http_fetch_url() and http_post() return values. */
#define HTTP_OK			0 
#define HTTP_FAILURE		-1 
#define HTTP_CERT_FAILURE	-2 

/* Default form content-type. */
#define HTTP_CONTENT_URLENCODED	"application/x-www-form-urlencoded"

#define HTTP_CONTENT_TYPE_HEADER "content-type"
#define HTTP_HOST_HEADER "host"

struct http_parameter {
	const char *name;
	const char *value;
	struct http_parameter *next;
};


char *http_url_encode(void *handle, const char *string);

/* Internal structure used for raw page data */
/**
 * struct http_buffer - buffer to store a page in
 * @buffer: pointer to allocated buffer
 * @max: maximum size of buffer
 * @cur: current length of content
 * @url: effective URL for transfer (after any redirects)
 * @base: the value of the 'href' attribute of the base HTML tag, if present
 * @response: HTTP response code
 */
struct http_buffer {
	char *data;
	unsigned int max;
	unsigned int cur;
	char *url;
	char *base;
	long response;
};

/**
 * http_header - an HTTP header
 * @name: a pointer to a nul terminated name field for the header
 * @value: a pointer to a nul terminated value field for the header
 */
struct http_header {
	const char *name;
	const char *value;
};

/**
 * http_headers - an array of HTTP headers
 * @count: the number of headers
 * @entries: a pointer to an array of 'count' http_headers
 */
struct http_headers {
	size_t count;
	struct http_header *entries;
};

/**
 * http_new_buffer - allocate a new buffer of the specified size
 * @handle: HTTP library handle
 * @size: size of the data buffer
 * Returns: Pointer to the newly allocated buffer, NULL on error
 */
static struct http_buffer *http_new_buffer(size_t size);

/**
 * http_free_buffer - free a previously allocated buffer
 * @handle: HTTP library handle
 * @buf: pointer to buffer
 */
static void http_free_buffer(struct http_buffer *buf);

/**
 * http_receive_data - callback for processing data received over HTTP
 * @ptr: pointer to the current chunk of data
 * @size: size of each element
 * @nmemb: number of elements
 * @arg: buffer to store chunk in
 * Returns: amount of data processed
 */
size_t http_receive_data(void *ptr, size_t size, size_t nmemb, void *arg);

/**
 * _http_fetch_url - fetch a URL into the specified buffer, internal function
 * @handle: HTTP library handle
 * @url: URL to fetch
 * @con_close: close the HTTP connection? (1=yes; 0=no)
 * @hdrs: a list of headers to be included in the request or NULL
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int _http_fetch_url(void *handle, const char *url,
		    int con_close,
		    struct http_headers *hdrs);

/**
 * _http_fetch_url_quiet - fetch a URL into the specified buffer, 
 * 	internal function, does not print debug info
 * @handle: HTTP library handle
 * @url: URL to fetch
 * @con_close: close the HTTP connection? (1=yes; 0=no)
 * @hdrs: a list of headers to be included in the request or NULL
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int _http_fetch_url_quiet(void *handle, const char *url,
			  int con_close,
			  struct http_headers *hdrs);

/**
 * http_fetch_url - fetch a URL into the specified buffer, system integration
 * 		    function
 * @handle: HTTP library handle
 * @url: URL to fetch
 * @con_close: close the HTTP connection? (1=yes; 0=no)
 * @headers: a list of headers to be included in the request
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int http_fetch_url(void *handle, const char *url,
		   int con_close,
		   struct http_headers *headers);

/**
 * _http_post - post a form back to the specified URL, internal function
 * @handle: HTTP handle
 * @url: URL to post to
 * @data: data string to send with post
 * @headers: a list of headers to be included in the request
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int _http_post(void *handle, const char *url,
		    const char *data,
		   struct http_headers *headers);

/**
 * _http_post_quiet - post a form back to the specified URL,
 * 	internal function, does not print debug info
 * @handle: HTTP handle
 * @url: URL to post to
 * @data: data string to send with post
 * @headers: a list of headers to be included in the request
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int _http_post_quiet(void *handle, const char *url,
		    const char *data,
		   struct http_headers *headers);

/**
 * http_post - post a form back to the specified URL, system integration
 * 		    function
 * @handle: HTTP handle
 * @url: URL to post to
 * @data: data string to send with post
 * @headers: a list of headers to be included in the request
 * Returns: HTTP_* result code (HTTP_OK, etc.)
 */
int http_post(void *handle, const char *url,
		   const char *data,
		   struct http_headers *headers);

/**
 * http_init - initialise an HTTP session
 * Returns: HTTP handle to use in other calls
 */
void *http_init();

/**
 * http_deinit - terminate an HTTP session
 * @handle: HTTP handle
 */
void http_deinit(void *handle);

/**
 * http_strerror - obtain user readable error string
 * @error: error code to look up
 * Returns: string version of error
 */
const char *http_strerror(int error);

/**
 * http_escape - encode a string in 'application/x-www-form-urlencoded' format
 * @str: NULL-terminated string to encode
 * Returns: allocated application/x-www-form-urlencoded version of string
 */
char *http_escape(const char *str);

/**
 * http_get_data - return pointer to raw data buffer (read-only)
 * @buf: pointer to buffer structure
 * @len: length of buffer (out)
 * Returns: const pointer to raw data
 */
const char *http_get_data(void *handle, int *len);

int http_get_response_code(void *handle);

/**
 * http_get_url - get the current real URL from a buffer
 * @handle: HTTP library handle
 * Returns: read-only pointer to URL
 */
const char *http_get_url(void *handle);

/**
 * http_reset_buffer - reset the pointers in a buffer
 * @buf: buffer to reset
 */
void http_reset_buffer(struct http_buffer *buf);

/**
 * http_clear_cookies - clear any cookies stored by the http library
 * @handle: HTTP handle
 */
void http_clear_cookies(void *handle);

int http_set_https_certificate_file(void *handle, const char *filename);

#endif /* _HTTP_H_ */
