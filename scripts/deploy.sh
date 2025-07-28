#!/bin/bash

# Adobe to Mixpanel ETL - Cloud Function Deployment Script
# This script deploys the ETL pipeline as a Google Cloud Function

set -e

echo "🚀 Starting deployment of adobe-transform Cloud Function..."

# Check if .env.yaml exists
if [ ! -f ".env.yaml" ]; then
    echo "❌ Error: .env.yaml file not found. Please create it with required environment variables."
    exit 1
fi

# Clean temporary files before deployment
echo "🧹 Cleaning temporary files..."
npm run prune

echo "📦 Deploying to Google Cloud Functions..."
gcloud functions deploy adobe-transform \
    --gen2 \
    --update-labels snowcat=transformer \
    --no-allow-unauthenticated \
    --env-vars-file .env.yaml \
    --runtime nodejs18 \
    --region us-central1 \
    --trigger-http \
    --memory 4GB \
    --entry-point start \
    --source . \
    --timeout=3600 \
    --max-instances=3355 \
    --min-instances=0 \
    --concurrency=1

echo "✅ Deployment completed successfully!"
echo "📊 Function URL: https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/adobe-transform"
echo "💡 Don't forget to update YOUR_PROJECT_ID with your actual project ID"