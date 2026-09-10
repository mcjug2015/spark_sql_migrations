#!/usr/bin/env bash
# Everything ci.yml does, locally: gates, then build and verify the three
# install flavours. Does not publish.
set -euo pipefail

here="$(dirname "${BASH_SOURCE[0]}")"
"$here/validate.sh" "$@"
"$here/build_and_verify.sh"
