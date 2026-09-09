# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Adobe-to-Mixpanel ETL pipeline that transforms Adobe Analytics raw hit data into Mixpanel event format. The pipeline processes TSV files (compressed or uncompressed) from local filesystem or Google Cloud Storage, applies lookup transformations, and outputs NDJSON format suitable for Mixpanel import.

## Architecture

### Core Processing Pipeline (index.js:89-274)
The main transformation pipeline consists of:
1. **File Input**: Supports local files or GCS URIs (`gs://bucket/path`)
2. **Stream Processing**: Uses Node.js streams for memory-efficient processing of large files
3. **CSV Parsing**: PapaParse with custom header transformation
4. **Data Transformation**: Adobe raw data → Mixpanel event format
5. **Output**: NDJSON file to local temp or GCS destination

### Key Components

**Lookup System (index.js:57-67)**:
- `lookups-standard/`: Adobe standard dimension lookups (browser, country, etc.)
- `lookups-custom/columns.csv`: Column header mappings for the raw data
- `lookups-custom/events.tsv`: Event ID to human-readable name mappings
- Customer-supplied evars, props, and custom events (passed via LOOKUPS parameter)

**Transform Functions**:
- `cleanAdobeRaw()` (index.js:370-459): Resolves raw Adobe values to human-readable using lookup tables
- `adobeToMixpanel()` (index.js:283-367): Converts Adobe hit data to Mixpanel event format
- Handles evar/prop resolution when customer lookups provided
- Generates insert_id from hitid_high/hitid_low for deduplication

**Entry Points**:
- `scratch.mjs`: Development entry point with Google Sheets integration for lookup data
- `function.js`: Google Cloud Functions HTTP entry point for production
- Direct import of `main()` function for programmatic use

## Common Development Commands

```bash
# Development with file watching and pretty logs
npm run dev

# Google Cloud Functions local development
npm run func

# Local deployment testing
npm run localDeploy
npm run localRun

# Production deployment
npm run deploy

# Clean temporary files
npm run prune

# Build request templates
npm run buildReq
```

## Key Data Flow Challenges

**Evar/Prop Resolution**: The pipeline transforms numeric evar/prop columns (e.g., `evar1`, `prop5`) to human-readable names using customer-supplied lookup data. The header transformation (index.js:149-194) handles this mapping during CSV parsing.

**Event List Processing**: Adobe's `event_list` contains comma-separated event IDs that need resolution through multiple lookup stages:
1. Standard event lookup (events.tsv)
2. Customer custom event lookup (if provided)
3. Value extraction for events with `=` notation (e.g., `704=20`)

**Product List Explosion**: Commented-out code (index.js:308-360) shows previous logic for "exploding" product data into separate events, which may be needed for ecommerce implementations.

## File Structure Notes

- `tmp/`: Temporary processing files (cleaned by `npm run prune`)
- `sample/`: Test data files for development
- `lookups-standard/`: Standard Adobe dimension lookups
- `lookups-custom/`: Required customer-supplied column and event mappings
- `guides/`: Legacy CSV guides (may be superseded by Google Sheets integration)

## Integration Points

**Google Sheets Integration** (scratch.mjs): Fetches lookup data from Google Sheets for dynamic configuration. Organizes data by category (MAIN, FOUNDATION, ADVANCE) and type (evars, props, custom_events).

**Google Cloud Storage**: Handles both input and output to GCS with automatic gzip compression/decompression.

**Logging**: Bunyan logger with Cloud Logging integration and local pretty-printing in development mode.

## Transform Behavior

- Null value handling: `""`, `"--"`, `"-"`, `":"` → `null`
- JSON parsing: Attempts to parse JSON strings in cell values (Adobe truncates at 1000 chars)
- User identification: Prefers `visid_high`/`visid_low` over `mcvisid` for distinct_id
- Timestamp: Uses `hit_time_gmt`, `cust_hit_time_gmt`, or `last_hit_time_gmt`
- Insert ID: Hash of `hitid_high`-`hitid_low` for deduplication (changed from `$insert_id` to `insert_id`)

## ETL Logic Updates (Recent Changes)

### Event Processing Strategy (index.js:325-412)

**Problem Solved**: Adobe Analytics fires many technical/measurement events that were creating noise in Mixpanel as separate events. Research into Adobe and Mixpanel best practices revealed that measurements should be properties, not events.

**Solution Implemented**:

1. **Event Classification**: Events are now classified into two categories:
   - **Measurement/Technical Events**: Converted to properties on the main event
   - **Business Events**: Remain as separate events

2. **Measurement Events** (converted to properties):
   ```javascript
   const propertyEvents = new Set([
     'Page Load Time',
     'Page Load Time Previous Page', 
     'Time Spent on Page',
     'Download Time',
     'Form Field Progress',
     'Instance of eVar11',
     'Instance of eVar32',
     'Filter',
     'Searchlight Content Health Score',
     'accordionExpanded',
     'accordionCollapse',
     'Ceros Component Click Event'
   ]);
   ```

3. **Business Events** (remain as separate events):
   - Link Click
   - Form Initialize  
   - Asset Download
   - Custom conversion events
   - User-initiated actions

**Event Value Handling**: Events with `=` notation (e.g., `209=31.12`) have their values preserved:
- As properties: `"page_load_time": "31.12"`
- As event properties: `"event_value": "31.12"`

### Data Structure Changes

**Flattened Output Format**: Removed nested `properties` object structure in favor of flat JSON:

**Before**:
```json
{
  "event": "Page Viewed",
  "properties": {
    "time": 1751384964,
    "distinct_id": "12345",
    "page_name": "Home"
  }
}
```

**After**:
```json
{
  "event": "Page Viewed", 
  "time": 1751384964,
  "distinct_id": "12345",
  "page_name": "Home",
  "page_load_time": "31.12",
  "form_field_progress": true
}
```

### Benefits

1. **Reduced Event Volume**: Measurement events no longer create separate records
2. **Cleaner Analytics**: Business events are easier to identify and analyze  
3. **Better Performance**: Fewer events means faster queries and lower costs
4. **Mixpanel Best Practices**: Follows recommended pattern of using properties for context
5. **Preserved Data**: All measurement data is retained as properties with values when available

### Hit Processing Flow

1. **Page View Hits** (`post_page_event === "0"`):
   - Create "Page Viewed" event
   - Add measurement properties directly to page view event
   - Create separate events for business actions

2. **Link Tracking Hits** (`post_page_event === "12"`):
   - Create events for business actions only
   - Measurements become properties on "Action Tracked" event if no business events exist

3. **Event Array Handling**: 
   - Always returns arrays for consistent processing
   - Maintains unique `insert_id` per event for deduplication
   - **Timestamp Nudging**: Events get +5 second intervals to create logical sequence

### Performance Optimizations (Recent Changes)

**Problem Solved**: Heavy lookup table loading was blocking Cloud Function startup and preventing effective caching.

**Solutions Implemented**:

1. **Shared Logger** (`logger.js`):
   - Centralized logging configuration for all modules
   - Consistent logging across `function.js`, `index.js`, and `scratch.mjs`
   - Proper development vs production stream handling

2. **Lazy Loading Strategy**:
   - Lookup tables only load on first request, not at module import
   - **function.js**: Caches `main()` function and `AGGREGATED_GUIDES`
   - **index.js**: Caches Adobe lookup tables (`lookups-standard/`, `columns.csv`, `events.tsv`)
   - Parallel loading with `Promise.all()` for optimal performance

3. **Fast Startup Benefits**:
   - Cloud Function container starts immediately
   - First request pays initialization cost (cached for subsequent requests)
   - Reduced cold start times
   - Better resource utilization

### Cache Behavior

- **First Request**: ~2-3 seconds (loads all lookup tables)
- **Subsequent Requests**: ~100-500ms (uses cached data)
- **Container Restart**: Cache rebuilds automatically on first new request