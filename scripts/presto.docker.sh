#!/usr/bin/env bash

docker run --rm -i ingomuellernet/presto:0.248.1 presto-cli "$@"
