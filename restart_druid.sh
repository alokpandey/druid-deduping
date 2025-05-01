#!/bin/bash

# Script to restart Druid services
LOG_FILE="$HOME/druid_restart.log"

echo "Restarting Druid services at $(date)" >> $LOG_FILE
docker restart druid-setup-druid-1 >> $LOG_FILE 2>&1

# Wait for Druid to fully start up
echo "Waiting for Druid services to start up..." >> $LOG_FILE
sleep 120

# Check if Druid is responding
if curl -s "http://localhost:8082/druid/v2/sql" -H "Content-Type: application/json" \
   -d '{"query":"SELECT 1"}' | grep -q "1"; then
  echo "Druid services successfully restarted at $(date)" >> $LOG_FILE
else
  echo "WARNING: Druid services may not have started properly at $(date)" >> $LOG_FILE
fi
