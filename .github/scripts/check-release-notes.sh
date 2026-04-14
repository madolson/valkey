#!/usr/bin/env bash
# Validates that a PR with the "release-notes" label includes a properly
# formatted entry in 00-RELEASENOTES.
#
# Usage: check-release-notes.sh <base-ref>
#   base-ref: the git ref to diff against (e.g. origin/unstable)

set -euo pipefail

BASE_REF="${1:?Usage: check-release-notes.sh <base-ref>}"
RELEASE_NOTES_FILE="00-RELEASENOTES"

VALID_SECTIONS="New Features and enhanced behavior
Command and API updates
Performance and Efficiency improvements
Module API changes
Observability and logging
Build and Tooling
Bug Fixes"

# Entry pattern: * <text> by @<author>(, @<author>)* (#<number>(, #<number>)*)
ENTRY_REGEX='^\* .+ by @[a-zA-Z0-9_-]+(, @[a-zA-Z0-9_-]+)* \(#[0-9]+(, #[0-9]+)*\)$'

errors=0

# Get the full diff for the release notes file.
diff_output=$(git diff "${BASE_REF}"...HEAD -- "${RELEASE_NOTES_FILE}" || true)

if [ -z "$diff_output" ]; then
    echo "ERROR: No changes found in ${RELEASE_NOTES_FILE}."
    echo "PRs with the 'release-notes' label must add an entry to ${RELEASE_NOTES_FILE}."
    exit 1
fi

# Extract only added lines (exclude diff headers).
added_lines=$(echo "$diff_output" | grep '^+' | grep -v '^+++' | grep -v '^+$' | sed 's/^+//')

if [ -z "$added_lines" ]; then
    echo "ERROR: No new lines added to ${RELEASE_NOTES_FILE}."
    exit 1
fi

# Filter to only entry lines (lines starting with *).
entry_lines=$(echo "$added_lines" | grep '^\* ' || true)

if [ -z "$entry_lines" ]; then
    echo "ERROR: No release note entries found in the added lines."
    echo "Entries must start with '* ' and follow the format:"
    echo "  * <description> by @<author> (#<PR>)"
    exit 1
fi

# Validate each entry line matches the expected format.
while IFS= read -r line; do
    if ! echo "$line" | grep -qE "$ENTRY_REGEX"; then
        echo "ERROR: Malformed release note entry:"
        echo "  $line"
        echo "Expected format:"
        echo "  * <description> by @<author> (#<PR>)"
        errors=$((errors + 1))
    fi
done <<< "$entry_lines"

# Validate that each entry appears under a valid section heading.
# Walk the file tracking the current section, then check added entries.
while IFS= read -r line; do
    section=$(awk -v entry="$line" '
        /^### / { section = substr($0, 5) }
        $0 == entry { print section; exit }
    ' "$RELEASE_NOTES_FILE")

    if [ -z "$section" ]; then
        continue
    fi

    if ! echo "$VALID_SECTIONS" | grep -qxF "$section"; then
        echo "ERROR: Entry is not under a valid section (found under '${section}'):"
        echo "  $line"
        echo "Valid sections:"
        echo "$VALID_SECTIONS" | sed 's/^/  - /'
        errors=$((errors + 1))
    fi
done <<< "$entry_lines"

if [ "$errors" -gt 0 ]; then
    echo ""
    echo "Found ${errors} release note error(s). Please fix and push again."
    exit 1
fi

echo "Release notes validation passed."
exit 0
