#!/bin/bash

# Adobe to Mixpanel ETL - Local Testing Script
# This script sends test data to the locally running Cloud Function

set -e

# Configuration
LOCAL_URL="http://localhost:8080"
SAMPLE_FILE="./sample/v-small-kf.tsv"

echo "🧪 Testing local Cloud Function..."


# Check if sample file exists
if [ ! -f "$SAMPLE_FILE" ]; then
    echo "❌ Error: Sample file not found at $SAMPLE_FILE"
    echo "💡 Please ensure you have a sample TSV file for testing"
    exit 1
fi

# Create test payload
# echo "📋 Creating test payload..."
# TEST_PAYLOAD=$(cat <<EOF
# {
#   "cloud_path": "$SAMPLE_FILE",
#   "dest_path": "./tmp/"
# }
# EOF
# )

echo "📋 Creating test payload..."
TEST_PAYLOAD=$(cat <<EOF
{  
"cloud_path": "gs://korn_ferry_adobe/s3_source/kf-main/01-kornferryproduction_2023-06-01.tsv.gz",  
"dest_path": "gs://korn_ferry_adobe/transformed/kf-main/" 
}
EOF
)

echo "📨 Sending POST request to local function..."
echo "📂 Processing file: $SAMPLE_FILE"
echo "📍 Destination: ./tmp/"
echo ""

# Send the request with pretty output
curl -X POST \
  -H "Content-Type: application/json" \
  -d "$TEST_PAYLOAD" \
  "$LOCAL_URL" \
  -w "\n\n📊 Response Status: %{http_code}\n⏱️  Response Time: %{time_total}s\n" \
  -s

echo ""
echo "✅ Test completed!"
echo "📁 Check ./tmp/ directory for output files"
echo "💡 Output file should be: ./tmp/v-small-kf.ndjson"