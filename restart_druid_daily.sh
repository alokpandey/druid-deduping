#!/bin/bash

# Script to restart Druid services daily
# This will be run as a background job

LOG_FILE="$HOME/druid_restart.log"

echo "Starting Druid restart service at $(date)" >> $LOG_FILE

while true; do
  # Restart Druid container
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
  
  # Sleep for 24 hours before next restart
  echo "Next restart scheduled for $(date -d '+24 hours')" >> $LOG_FILE
  sleep 86400  # 24 hours in seconds
done
