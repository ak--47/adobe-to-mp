#!/bin/bash

# Adobe to Mixpanel ETL - Local Development Script
# This script runs the Cloud Function locally for development and testing

set -e

echo "🏠 Starting local Cloud Function server..."

# Check if .env.yaml exists
if [ ! -f ".env.yaml" ]; then
    echo "❌ Error: .env.yaml file not found. Please create it with required environment variables."
    exit 1
fi

# Clean temporary files
echo "🧹 Cleaning temporary files..."
npm run prune

# Load environment variables from .env.yaml if needed
# Note: functions-framework will automatically load NODE_ENV and other vars

echo "🔧 Starting functions-framework on port 8080..."
echo "📡 Function will be available at: http://localhost:8080"
echo "🛑 Press Ctrl+C to stop the server"
echo ""

# Start the functions framework with pretty logging
npx functions-framework --target=start --source . --signature-type=http --port=8080 | npx bunyan -o short