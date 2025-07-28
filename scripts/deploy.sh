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
# echo "🧹 Cleaning temporary files..."
# npm run prune

echo "📦 Deploying to Google Cloud Functions..."
gcloud functions deploy adobe-transform \
    --gen2 \
    --update-labels snowcat=transformer \
    --no-allow-unauthenticated \
    --env-vars-file .env.yaml \
    --runtime nodejs20 \
    --region us-central1 \
    --trigger-http \
    --memory 8GB \
    --entry-point start \
    --source . \
    --timeout=3600 \
    --max-instances=3355 \
    --min-instances=0 \
    --concurrency=1

echo "✅ Deployment completed successfully!"
echo "📊 Function URL: https://adobe-transform-lmozz6xkha-uc.a.run.app"
echo "💡 Don't forget to update mixpanel-gtm-training with your actual project ID"