#!/usr/bin/env bash

set -eu

script=""
for i in $(seq 1 200); do
  script="$script\ncreate table products$i (userId string);"
done
echo -e $script | target/universal/stage/bin/dynamite --endpoint http://localhost:8080
