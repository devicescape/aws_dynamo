dnl Process this file with autoconf to produce a configure script.
AC_INIT(aws_dynamo, 0.1.0)
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_SRCDIR(src/aws_dynamo.c)

AM_INIT_AUTOMAKE([foreign])

AC_CHECK_LIB([crypto], [HMAC_Init_ex],,
				 AC_MSG_ERROR([no openssl; please install openssl or equivalent]))
AC_CHECK_LIB([yajl], [yajl_alloc],,
				 AC_MSG_ERROR([no libyajl; please install yajl or equivalent]))

# We support two yajl major versions. Version 1.x.x doesn't include yajl_version.h.
# If yajl_version.h is missing assume major version 1.  Otherwise we will get the
# major version from the header.
AC_CHECK_HEADERS([yajl/yajl_version.h], [], [ AC_DEFINE(YAJL_MAJOR, 1, [yajl version 1])], [])

AC_CHECK_LIB([curl], [curl_easy_perform],,
				 AC_MSG_ERROR([no curl; please install curl or equivalent]))

LOCAL_CFLAGS="-Wall -Wno-pointer-sign"
AC_ARG_ENABLE([debug], [  --enable-debug    Turn on debugging],
[case "${enableval}" in
  yes)
    debug=true
    LOCAL_CFLAGS="${LOCAL_CFLAGS} -Werror -DDEBUG_AWS_DYNAMO -DDEBUG_PARSER -DDEBUG_AWS_KINESIS"
    ;;
  no)
    debug=false
    ;;
  *) AC_MSG_ERROR([bad value ${enableval} for --enable-debug]) ;;
esac],[debug=false])
AM_CONDITIONAL([DEBUG], [test x$debug = xtrue])
 
AC_SUBST([AM_CFLAGS], [${LOCAL_CFLAGS}])

AC_PROG_CC
AC_PROG_LIBTOOL

AC_OUTPUT(Makefile src/Makefile test/Makefile)
