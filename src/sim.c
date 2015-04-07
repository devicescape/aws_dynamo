/*
 * Copyright (c) 2006-2015 Devicescape Software, Inc.
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
 *
 */

#ifdef AWS_DYNAMO_HTTP_SIM

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#include <sys/types.h>
#include <regex.h>

struct file_info {
	char *name;
	int count;
};

static struct file_info *files;
static int num_files = 0;

/**
 * sim_read_file - read the contents of a file into an allocated buffer
 * Returns: allocated string containing the file contents with trailing
 * 	    whitespace removed, NULL on failure
 */
char *
sim_read_file(const char *filename)
{
	FILE *fp = NULL;
	char *contents = NULL;
	struct stat statbuf;
	int len;

	if (stat(filename, &statbuf) != 0) {
		goto error;
	}

	contents = calloc(statbuf.st_size + 1, 1);

	if (contents == NULL) {
		perror("calloc");
		goto error;
	}

	fp = fopen(filename, "r");

	if (fp == NULL) {
		perror("fopen");
		goto error;
	}

	if (statbuf.st_size &&
	    fread(contents, statbuf.st_size, 1, fp) != 1) {
		perror("fread");
		goto error;
	}

	fclose(fp);

	/* Remove trailing whitespace */
	len = strlen(contents);

	while (len > 0 && isspace(contents[len - 1])) {
		contents[len - 1] = '\0';
		len = strlen(contents);
	}

	return contents;

error:
	if (fp)
		fclose(fp);
	free(contents);
	return NULL;
}

/**
 * sim_regmatch - perform a regular expression match
 * @string: the string to search
 * @regex: the regular expression to search for
 * Returns: 0 on match, 1 on no match, -1 on error
 */
int sim_regmatch(char *string, char *regex)
{
	regex_t reg;
	regmatch_t match[2];
	int errcode;

	errcode = regcomp(&reg, regex, 0);

	if (errcode != 0) {
		char errbuf[256];

		regerror(errcode, &reg, errbuf, sizeof (errbuf));
		printf("regcomp: %s\n", errbuf);
		regfree(&reg);
		return -1;
	}

	errcode =
	    regexec(&reg, string, sizeof (match) / sizeof (match[0]), match, 0);

	if (errcode != 0 && errcode != REG_NOMATCH) {
		char errbuf[256];

		regerror(errcode, &reg, errbuf, sizeof (errbuf));
		printf("regex: %s\n", errbuf);
		regfree(&reg);
		return -1;
	}
	regfree(&reg);

	if (errcode == 0)
		return 0;

	return 1;
}

/**
 * get_matching_length - gets the # of matching chars of a url against a template
 * @templ: the template to match against. % is a wildcard character.
 * @url: the URL to match.
 * Returns: the number of leading characters of url that match templ.
 */
static int get_matching_length(const char *templ, const char *url)
{
	int i = 0;

	if (templ == NULL || url == NULL) {
		return -1;
	}

	while (*templ != '\0' && *url != '\0'
	       && ( *templ == *url || *templ == '%' ) ) {
		templ++;
		url++;
		i++;
	}

	return i;
}

/**
 * sim_url_escape - remove characters from a URL that aren't portable
 *                  across filesystems
 *
 * @url: the URL
 */
void sim_url_escape(char *url)
{
	char *p;

	for (p = url; *p != '\0'; p++)
		if (*p == '/')
			*p = '~';
		else if (*p == ':')
			*p = '~';
		else if (*p == '?')
			*p = '~';
}

/**
 * sim_get_best_match - get the filename that best matches a URL
 * @url: the URL to match.
 * @filename: pointer to a string that (on success) will be set to the filename
 *            in the current directory that best matches url.
 * @inc: if zero do not increment file count, otherwise do increment file count
 * Returns: 0 on success; -1 on failure.
 */
int sim_get_best_match(const char *url, char **filename, int inc)
{
	char *url_escaped;
	int max_match = 0;
	int max_index = -1; /* The index of the file with the maximum match. */
	int i;

	if (files == NULL) {
		/* Build a cache of files in the current directory. */
		struct dirent **namelist;

		num_files = scandir(".", &namelist, NULL, alphasort);

		files = calloc(num_files, sizeof(*files));
		if (files == NULL) {
			perror("calloc");

			for (i = 0; i < num_files; i++)
				free(namelist[i]);
			free(namelist);
			*filename = NULL;
			return -1;
		}

		for (i = 0; i < num_files; i++) {
			files[i].name = strdup(namelist[i]->d_name);
			free(namelist[i]);
		}

		free(namelist);
	}

 	url_escaped = strdup(url); 
 	sim_url_escape(url_escaped);

	fprintf(stderr, "Matching %s\n", url_escaped);

	for (i = 0; i < num_files; i++) {
		int match;

		/* Ignore meta files */
		if (strstr(files[i].name, ".effective"))
			continue;
		if (strstr(files[i].name, ".alive"))
			continue;
		if (strstr(files[i].name, ".redir"))
			continue;
		if (sim_regmatch(files[i].name, "\\.[0-9][0-9]*$") == 0) {
			continue;
		}

		match = get_matching_length(files[i].name, url_escaped);
		printf(" %2d %s\n", match, files[i].name);
		if (match > max_match) {
			max_match = match;
			max_index = i;
		}
	}

	free(url_escaped);

	if (max_index >= 0) {
		struct stat statbuf;
		char sequence_file[PATH_MAX];

		if (inc)
			files[max_index].count++;

		/* Look to see if a sequence file should be used instead of
		 * the plain URL file. */
		snprintf(sequence_file, sizeof(sequence_file), "%s.%d",
			 files[max_index].name,
			 files[max_index].count);

		if (stat(sequence_file, &statbuf) == 0) {
			*filename = strdup(sequence_file);
		} else {
			*filename = strdup(files[max_index].name);
		}

		printf("Matched %s\n", *filename);
	} else {
		printf("No match\n");
		*filename = NULL;
	}

	return 0;
}

/**
  * sim_deinit - cleanup resources
  */
void sim_deinit(void)
{
	int i;

	for (i = 0; i < num_files; i++) {
		free(files[i].name);
	}
	free(files);
}

#endif /* AWS_DYNAMO_HTTP_SIM */
