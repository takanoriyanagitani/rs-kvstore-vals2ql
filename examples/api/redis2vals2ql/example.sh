#!/bin/sh

export REDIS_URL="redis://localhost:6379"
export ADDR_PORT="127.0.0.1:8029"

./redis2vals2ql
