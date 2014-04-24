#!/bin/sh
set -eux

VERSION=$1

THISDIR="$( cd "$( dirname "$0" )" && pwd )"
cd $THISDIR
cd ..
rm -rf ds-release
mkdir ds-release

export GIT_PAGER=''

git status
CHANGES=`git status | egrep 'modified|removed|changed|Changes|Untracked|ahead|behind'| wc -l`
if [ "$CHANGES" != "0" ] ; then
    echo "Cleaning up local changes."
    git add -A
    git diff --cached
    git reset --hard
    git clean -f
    git checkout master #We are just using the master branch for now.
    git branch
fi
git pull --rebase
CHANGES=`git status | egrep 'modified|removed|changed|Changes|Untracked|ahead|behind'| wc -l`
if [ "$CHANGES" != "0" ] ; then
    echo "Local changes, aborting."
    #echo "FIXME!!! Skipping changes for debugging"; exit 0
    exit 1
fi

git log -n1 --stat

chmod +w pkg/aws_dynamo.spec

sed -e "s/^Version: .*/Version: $VERSION/; " \
	< pkg/aws_dynamo.spec \
	> pkg/aws_dynamo.spec.tmp
mv pkg/aws_dynamo.spec.tmp pkg/aws_dynamo.spec

$THISDIR/rpm-buildpackage pkg/aws_dynamo.spec

