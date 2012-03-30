#!/bin/sh

# purge everything first
rm -rf m4/ autom4te.cache/
rm -f depcomp install-sh missing ltmain.sh

# then recreate it, just to be sure
mkdir m4/
autoreconf --install --verbose
