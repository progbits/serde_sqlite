#!/bin/bash

set -o -u -f pipefail

current_dir=$(pwd)
project_root=$(git rev-parse --show-toplevel)

cd "$project_root" || exit

for i in $(seq 65536); do
  cargo test --tests -- --nocapture
  if [[ $? -ne 0 ]]; then
    break
  fi
done

cd "$current_dir"
