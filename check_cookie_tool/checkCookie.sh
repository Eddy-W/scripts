#!/bin/sh
BINDIR=$(dirname "$0")
SMAUSER=anson

if [ -r $BINDIR/env.sh ]; then
	. $BINDIR/env.sh
fi

if [ $(id -un) != $SMAUSER ]; then
	SUDO="sudo -H -u $SMAUSER"
else
	SUDO=
fi
exec $SUDO $BINDIR/checkCookie.py "$@"
