#!/bin/sh

export RUST_BACKTRACE=1
export LD_LIBRARY_PATH=__DEPLOY_DIR__/bin/tiflash:$LD_LIBRARY_PATH

exec __DEPLOY_DIR__/bin/tiflash/tiflash $@
