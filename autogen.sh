#!/bin/sh

aclocal  &&\
libtoolize --force --copy &&\
automake --add-missing --copy &&\
autoconf

