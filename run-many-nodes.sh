#!/bin/bash

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <count>"
  exit 1
fi

for ((i = 1; i <= $1; i++)); do
  ./run-node.sh "$i"
done
