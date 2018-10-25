#!/bin/bash

SCRIPT_DIR=$(cd $(dirname $(readlink $0 || echo $0));pwd)

p=`ps aux | grep jpx_loader | grep -v grep | wc -l`
if [ $p -ge 2 ]; then
  echo previous job is sttill running. skip this time.
  exit 0
fi

source $SCRIPT_DIR/venv/bin/activate
python $1
exit $?
