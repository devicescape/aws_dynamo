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

#ifndef _AWS_DYNAMO_UTILS_H_
#define _AWS_DYNAMO_UTILS_H_

#include <stdio.h>
#include <string.h>
#include <ctype.h>

#ifdef HAVE_YAJL_YAJL_VERSION_H
#include <yajl/yajl_version.h>
#endif

#ifdef DEBUG
#define Debug(args...) {fprintf (stderr, "DEBUG: "); fprintf (stderr, args); fprintf (stderr, "\n");}
#define Warnx(args...) {fprintf (stderr, "WARN:  "); fprintf (stderr, args); fprintf (stderr, "\n");}
#define Err(args...) {fprintf (stderr, "ERROR: "); fprintf (stderr, args); fprintf (stderr, "\n");}
#define Errx(args...) {fprintf (stderr, "ERROR: "); fprintf (stderr, args); fprintf (stderr, "\n");}
#else
#include <syslog.h>
#define Debug(args...) {syslog(LOG_DEBUG, args);} 
#define Warnx(args...) {syslog(LOG_WARNING, args);} 
#define Err(args...) {syslog(LOG_ERR, args);} 
#define Errx(args...) {syslog(LOG_ERR, args);} 
#endif /* DEBUG */

#endif /* _AWS_DYNAMO_UTILS_H_ */
