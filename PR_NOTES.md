# PR: Reduce Salesforce API Consumption

## Summary
Optimized tap-salesforce to significantly reduce API quota consumption through caching, optimized polling, rate limiting, and reduced concurrency.

## Changes

### API Consumption Optimizations
- **Caching**: Added caching for describe calls (1 hour TTL) and bulk quota checks (5 minutes TTL)
- **Polling Optimization**: Increased batch status polling intervals with exponential backoff
  - Regular batches: 20s → 30s initial, exponential backoff up to 2min
  - PK chunked batches: 60s → 90s initial, exponential backoff up to 5min
  - Bulk2 polling: 20s → 30s initial, exponential backoff up to 2min
- **Rate Limiting**: Added 100ms minimum interval between requests (max 10 req/sec)
- **Reduced Concurrency**: Default `max_workers` reduced from 8 to 4
- **REST API**: Optimized pagination to clear params after first request

### API Quota Logging
- Added quota logging at connection start and end
- Logs both REST and Bulk API quota usage with percentages
- Shows job-specific usage (REST requests, Bulk jobs completed)

### Logging Improvements
- Reduced request logging verbosity (moved to DEBUG level)
- Added pagination statistics for REST API

## Expected Impact
- **Discovery**: 50-90% reduction in API calls
- **Sync**: 30-60% reduction in API calls
- **Long-running jobs**: 50-60% reduction in polling calls

## Backward Compatibility
✅ All changes are backward compatible. Default behavior improved without breaking changes.

