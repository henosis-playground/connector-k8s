#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <project-name> <description> [repository-url]"
  echo "Example: $0 my-project 'A cool Rust project' 'https://github.com/skuld-systems/my-project'"
  exit 1
fi

PROJECT="$1"
DESCRIPTION="$2"
REPOSITORY="${3:-https://github.com/skuld-systems/$PROJECT}"

find . -not -path './.git/*' -not -path './.jj/*' -not -path './target/*' -type f -exec \
  sed -i -e "s|PROJECT|$PROJECT|g" -e "s|DESCRIPTION|$DESCRIPTION|g" -e "s|REPOSITORY|$REPOSITORY|g" {} +

echo "Initialized $PROJECT. You can delete this script now."
