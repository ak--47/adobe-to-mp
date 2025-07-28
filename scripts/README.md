# Adobe to Mixpanel ETL - Deployment Scripts

This directory contains organized scripts for deploying and testing the Adobe to Mixpanel ETL pipeline.

## Scripts Overview

### 🚀 `deploy.sh` - Production Deployment
```bash
npm run deploy
```
- Deploys the ETL pipeline as a Google Cloud Function
- Cleans temporary files before deployment
- Uses `.env.yaml` for environment variables
- Configures proper memory, timeout, and scaling settings

### 🏠 `local.sh` - Local Development Server
```bash
npm run local
```
- Runs the Cloud Function locally on port 8080
- Perfect for development and debugging
- Uses functions-framework with pretty logging
- Automatically loads environment variables

### 🧪 `test.sh` - Local Testing
```bash
npm run test
```
- Sends test data to the locally running function
- Uses sample data from `./sample/v-small-kf.tsv`
- Provides detailed response information
- Validates the complete ETL pipeline

## Development Workflow

1. **Start Local Server**:
   ```bash
   npm run local
   ```

2. **Test the Function** (in another terminal):
   ```bash
   npm run test
   ```

3. **Deploy to Production**:
   ```bash
   npm run deploy
   ```

## VS Code Integration

The launch configuration includes:
- `local-function`: Starts the local Cloud Function server
- `scratch`: Runs the development script with file watching
- `go`: Generic Node.js launcher for any file

## Environment Setup

Make sure you have `.env.yaml` configured with required environment variables before running local or deploy scripts.

## File Structure After Deployment

Only essential files are deployed to Cloud Functions:
- `function.js` (entry point)
- `index.js` (main ETL logic)  
- `korn-ferry-guides.js` (lookup data)
- `lookups-standard/` (Adobe dimension lookups)
- `lookups-custom/` (customer mappings)
- `package.json` and `node_modules/`

All development files, samples, and documentation are excluded via `.gcloudignore`.