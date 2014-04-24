#!/bin/sh
THISDIR="$( cd "$( dirname "$0" )" && pwd )"

VERSION=$1

cd $THISDIR
cd ..
rm -rf ds-release
mkdir ds-release

chmod +w pkg/aws_dynamo.spec

sed -e "s/^Version: .*/Version: $VERSION/; " \
	< pkg/aws_dynamo.spec \
	> pkg/aws_dynamo.spec.tmp
mv pkg/aws_dynamo.spec.tmp pkg/aws_dynamo.spec

$THISDIR/rpm-buildpackage pkg/aws_dynamo.spec

