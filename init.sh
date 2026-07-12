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

if ! [[ "$PROJECT" =~ ^[a-z][a-z0-9_-]*$ ]]; then
  echo "error: project name must match ^[a-z][a-z0-9_-]*\$, got: $PROJECT" >&2
  exit 1
fi

# Substitute {{...}} markers. \Q...\E treats the marker literally, and the
# replacement is an interpolated string, never re-parsed as a pattern — so the
# values are safe to contain any characters. This script is excluded so it
# survives its own run.
export PROJECT DESCRIPTION REPOSITORY
find . -not -path './.git/*' -not -path './.jj/*' -not -path './target/*' \
  -not -name 'init.sh' -type f -exec \
  perl -i -pe 's/\Q{{PROJECT}}\E/$ENV{PROJECT}/g; s/\Q{{DESCRIPTION}}\E/$ENV{DESCRIPTION}/g; s/\Q{{REPOSITORY}}\E/$ENV{REPOSITORY}/g' {} +

if grep -rIn --exclude-dir={.git,.jj,target} --exclude='init.sh' -F -e '{{PROJECT}}' -e '{{DESCRIPTION}}' -e '{{REPOSITORY}}' .; then
  echo "error: unreplaced placeholders remain (see above)" >&2
  exit 1
fi

echo "Initialized $PROJECT. You can delete this script now."
