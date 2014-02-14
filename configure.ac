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
AC_CHECK_LIB([curl], [curl_easy_perform],,
				 AC_MSG_ERROR([no curl; please install curl or equivalent]))
AC_CHECK_LIB([expat], [XML_ParserCreate],,
				 AC_MSG_ERROR([no libexpat; please install libexpat or equivalent]))

LOCAL_CFLAGS="-Wall -Wno-pointer-sign"
AC_ARG_ENABLE([debug], [  --enable-debug    Turn on debugging],
[case "${enableval}" in
  yes)
    debug=true
    LOCAL_CFLAGS="${LOCAL_CFLAGS} -Werror"
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
