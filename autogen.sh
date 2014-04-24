#!/bin/sh

mkdir -p config
mkdir -p m4
autoreconf --force --install -I config -I m4
