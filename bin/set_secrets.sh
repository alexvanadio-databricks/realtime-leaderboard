#!/usr/bin/env bash
# set-kafka-secrets.sh
# Usage: ./set-kafka-secrets.sh <KAFKA_USER> <KAFKA_PASSWORD> <DATABRICKS_PROFILE>

set -euo pipefail

if [ $# -ne 3 ]; then
echo "Usage: $0 <KAFKA_USER> <KAFKA_PASSWORD> <DATABRICKS_PROFILE>" >&2
exit 1
fi

KAFKA_USER="$1"
KAFKA_PASSWORD="$2"
PROFILE="$3"

SCOPE="realtime_leaderboard"
KEY_USER="kafka_user"
KEY_PASS="kafka_password"

command -v databricks >/dev/null 2>&1 || { echo "databricks CLI not found in PATH" >&2; exit 1; }

# Write via STDIN so values don't land in process args
printf '%s' "$KAFKA_USER"     | databricks -p "$PROFILE" secrets put-secret "$SCOPE" "$KEY_USER"
printf '%s' "$KAFKA_PASSWORD" | databricks -p "$PROFILE" secrets put-secret "$SCOPE" "$KEY_PASS"

echo "✅ wrote $SCOPE/$KEY_USER and $SCOPE/$KEY_PASS (profile=$PROFILE)"
