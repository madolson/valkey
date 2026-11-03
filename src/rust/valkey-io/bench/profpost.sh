#!/bin/bash
# Post-process a profile.sh recording: self-time top list, groups, flamegraph.
#   profpost.sh <name.data>...
HERE=$(cd "$(dirname "$0")" && pwd); FG=${FG:-$HOME/git/FlameGraph}
for D in "$@"; do
  B=${D%.data}
  sudo perf report -f -i "$D" --no-children --sort sym -g none --stdio 2>/dev/null | grep -E '^ +[0-9.]+%' | head -30 > "$B.top.txt"
  sudo perf script -f -i "$D" 2>/dev/null > "$B.script"
  python3 "$HERE/profgroup.py" "$B.script" > "$B.groups.txt"
  if [ -x "$FG/stackcollapse-perf.pl" ]; then
    "$FG/stackcollapse-perf.pl" "$B.script" > "$B.folded"
    "$FG/flamegraph.pl" --title "$(basename "$B")" "$B.folded" > "$B.svg"
  fi
  rm -f "$B.script"
done
