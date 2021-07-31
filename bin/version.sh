#!/bin/sh

if [ "x" = "x$1" ]; then
    echo >&2 "Version argument from .version file is required!"
    exit 1
fi

version=`cat "$1"`
thedate=`date +"%Y%m%d%H%M%S"`
commit_id=`git log --pretty=oneline | head -1 | awk '{ print $1 }'`
buildid="${commit_id}-${thedate}"

cat <<EOF
package version

var (
    VERSION        string = "$version"
    BUILDID        string = "$buildid"
)

EOF
