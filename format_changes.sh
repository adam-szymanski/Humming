#!/bin/bash
set -e
set -x

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "$SCRIPT_DIR"

git diff --diff-filter=d --name-only origin/main | grep -E '(\.h|\.c|\.hpp|\.cpp|\.impl|\.proto)$' | xargs clang-format-14 -style=file -i
git diff --diff-filter=d --name-only origin/main | grep -E '(CMakeLists|\.cmake$)' | xargs cmake-format -c "$SCRIPT_DIR/.cmake-format.yaml" -i
