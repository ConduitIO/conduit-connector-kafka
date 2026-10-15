#!/usr/bin/env bash
# Copyright © 2026 Meroxa, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Checks an acceptance suite v2 report (sdk.AcceptanceReport). Fails unless
# the report exists, the run finished, it is complete (no filtered run, no
# unexplained skip) and it passed. A missing report is a failed run.
#
# usage: scripts/check-acceptance-report.sh <report.json> [suite revision]
set -euo pipefail

report=${1:?usage: $0 <report.json> [suite revision]}
revision=${2:-2.0.0}

if [ ! -s "$report" ]; then
  echo "acceptance report $report is missing or empty: the v2 suite did not run" >&2
  exit 1
fi

jq -r '"suite \(.suite_version) rev \(.suite_revision) sdk \(.sdk_version): finished=\(.finished) complete=\(.complete) passed=\(.passed) floor_met=\(.limits.certification_floor_met) below_floor=\(.limits.below_floor // []) exceptions=\(.exceptions // [])"' "$report"
jq -r '.cases[] | select(.outcome != "passed") | "  \(.id) \(.outcome) \(.skip_kind // "") \(.reason // "")"' "$report"

jq -e --arg rev "$revision" '
  .finished == true and .complete == true and .passed == true and .suite_revision == $rev
' "$report" >/dev/null || {
  echo "acceptance report $report: not a finished, complete, passing run of revision $revision" >&2
  exit 1
}
