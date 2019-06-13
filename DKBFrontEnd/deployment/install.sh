#!/bin/sh -e
# $Id: install.sh 310 2017-04-14 15:07:26Z rea $

# Tunables
VERSION="6.10.2"
BASE=/opt/node.js
PREFIX="$BASE"/node
ONTODIA="$BASE"/ontodia
# 3 zero-padded digits for major, minor and patchlevel.
GCC_MIN="004006000"


# Nuts'n'bolts, edit with care
SRC="https://nodejs.org/dist/v${VERSION}/node-v${VERSION}.tar.xz"
ONTODIA_SRC="https://github.com/ontodia-org/ontodia.git"
DIR="node-v${VERSION}"
TARFLAGS="-J"


modern_gcc_instructions () {
	if [ -f /etc/issue ] && grep -q '^CentOS[[:space:]]' /etc/issue; then
		cat << EOF

Looks like we are running on CentOS.  Here is how to get recent GCC:
 - yum install -y centos-release-scl
 - yum install -y devtoolset-6-gcc-c++
 - scl enable devtoolset-6 bash
and rerun me from the activated shell.
EOF
		return
	fi
}


# Determine if we have proper GCC
GCC_VERSION="$( gcc --version 2>/dev/null | head -1 | sed -Ee's/^gcc\s+\([^\)]+\)\s+([0-9]+).([0-9]+).([0-9]+).*$/\1 \2 \3/' )"
if [ "$?" != 0 -o -z "$GCC_VERSION" -o \
   -n "$(echo "$GCC_VERSION" | tr -d '[ 0-9]')" ]; then
	cat >&2 << EOF
Was not able to extract GCC version from
{{{
$(gcc --version 2>&1)
}}}
EOF
	exit 1
fi
GCC_VERSION="$( echo "$GCC_VERSION" | (read maj min patch; printf "%03d%03d%03d" "$maj" "$min" "$patch") )"
if [ "$GCC_VERSION" -lt "$GCC_MIN" ]; then
	echo "Too old GCC, need at least ${GCC_MIN}." >&2
	modern_gcc_instructions
	exit 1
fi


# Some acrobatics with temporary directory.
TMPDIR=
trap '[ -n "$TMPDIR" ] && rm -rf "$TMPDIR"; trap "" 0; exit;' 0 1 2 3 15
TMPDIR="$(mktemp -d)"
if [ -z "$TMPDIR" ]; then
	echo "Can't create temporary directory, exiting." >&2
	exit 1
fi
t="$(readlink -f "$TMPDIR")"
if [ "$?" != 0 ]; then
	echo "Can't get full path of created temporary directory '$TMPDIR'." >&2
	exit 1
fi
TMPDIR="$t"
cd "$TMPDIR" || exit 1


## Node.js itself
curl -o src "$SRC"
tar -xvf src "$TARFLAGS"
cd "$DIR"
./configure --prefix="$PREFIX"
make
mkdir -p "$PREFIX"
make install
export PATH="$PREFIX"/bin:"$PATH"


## Ontodia
mkdir -p "$ONTODIA"
cd "$ONTODIA"
git clone "$ONTODIA_SRC" git
cd git
# Install all dependencies locally ...
npm install
# ... and build.
npm run build


# That's all, folks!
cat << EOF

========================================================================
Installed node.js and Ontodia.  Don't forget to prepend
  ${PREFIX}/bin
to our PATH to use NPM and stuff.

You can run
SPARQL_ENDPOINT=https://library-ontodia-org.herokuapp.com/sparql npm run demo
from Ontodia directory (${ONTODIA}/git) to invoke demo.

To deploy applications just use npm from this node.js installation
in a usual fashion.

Good luck!
========================================================================
EOF
