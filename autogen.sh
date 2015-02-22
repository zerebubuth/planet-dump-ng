#!/bin/sh

aclocal -I aclocal
autoheader
automake -a
autoconf
