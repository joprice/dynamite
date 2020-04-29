#!/usr/bin/env bash

set -eu

run() {
  echo -e $1 | target/universal/stage/bin/dynamite --endpoint http://localhost:8080
}

run "create table products (userId string);"

script=""
for i in $(seq 1 200); do
  script="$script\ninsert into products (userId) values (\"user-id-$i\");"
done

run "$script"
