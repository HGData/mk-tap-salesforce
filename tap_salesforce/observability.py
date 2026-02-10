"""Observability module for structured logging and metrics.

This module provides functions to emit structured log events for Datadog
integration. All logs are emitted in a format compatible with Datadog's
log parsing and faceting capabilities.

Metric events include a 'dd' namespace with metric_name, metric_value,
and metric_type fields for Datadog Log-Based Metric extraction.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import singer

if TYPE_CHECKING:
    from tap_salesforce.salesforce import Salesforce

LOGGER = singer.get_logger()


def get_tenant_id() -> str | None:
    """Get tenant ID from environment variable."""
    return os.environ.get("TENANT")


def _dd_metric(name: str, value: float, metric_type: str = "gauge") -> dict:
    """Build Datadog Log-Based Metric fields.

    Args:
        name: Metric name (e.g. 'mdi.salesforce.api.pre_extract')
        value: Numeric metric value
        metric_type: One of 'gauge', 'count'

    Returns:
        Dict with 'dd' namespace for Datadog extraction.
    """
    return {"dd": {"metric_name": name, "metric_value": value, "metric_type": metric_type}}


def log_sync_start(
    sf: "Salesforce",
    catalog: dict,
    config: dict,
) -> None:
    """Log structured event at the start of a sync operation.

    Args:
        sf: Salesforce client instance
        catalog: The catalog containing streams to sync
        config: The configuration dictionary
    """
    streams = catalog.get("streams", [])
    selected_streams = [s["stream"] for s in streams]

    log_data = {
        "event_type": "salesforce_sync_start",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tenant_id": get_tenant_id(),
        "api_type": sf.api_type,
        "streams_count": len(selected_streams),
        "streams": selected_streams[:20],  # Limit to first 20 to avoid huge logs
        "quota_percent_total_limit": sf.quota_percent_total,
        "quota_percent_per_run_limit": sf.quota_percent_per_run,
        "max_workers": config.get("max_workers", 8),
        "is_sandbox": sf.is_sandbox,
        "instance_url": sf.instance_url if hasattr(sf, "instance_url") else None,
    }

    # Log as JSON for Datadog parsing
    LOGGER.warning(json.dumps(log_data))


def log_sync_complete(
    sf: "Salesforce",
    config: dict,
    success: bool = True,
    error: Exception | None = None,
    records_synced: int | None = None,
) -> None:
    """Log structured event at the end of a sync operation.

    Args:
        sf: Salesforce client instance
        config: The configuration dictionary
        success: Whether the sync completed successfully
        error: Optional exception if sync failed
        records_synced: Optional count of records synced
    """
    log_data = {
        "event_type": "salesforce_sync_complete",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tenant_id": get_tenant_id(),
        "success": success,
        "api_type": sf.api_type,
        "rest_requests_attempted": sf.rest_requests_attempted,
        "bulk_jobs_completed": sf.jobs_completed,
        "quota_percent_total_limit": sf.quota_percent_total,
        "quota_percent_per_run_limit": sf.quota_percent_per_run,
        **_dd_metric("mdi.salesforce.api.requests_by_job", sf.rest_requests_attempted, "count"),
    }

    if error:
        log_data["error_type"] = type(error).__name__
        log_data["error_message"] = str(error)[:500]  # Limit error message length

    if records_synced is not None:
        log_data["records_synced"] = records_synced

    # Log as JSON for Datadog parsing
    LOGGER.warning(json.dumps(log_data))


def log_quota_status(
    sf: "Salesforce",
    used: int,
    allotted: int,
    phase: str = "current",
) -> None:
    """Log current quota status for monitoring.

    Args:
        sf: Salesforce client instance
        used: Number of API calls used
        allotted: Total API calls allotted
        phase: One of 'pre_extract', 'post_extract', 'current'
    """
    percent_used = (used / allotted * 100) if allotted > 0 else 0

    log_data = {
        "event_type": "salesforce_quota_status",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tenant_id": get_tenant_id(),
        "api_type": sf.api_type,
        "phase": phase,
        "quota_used": used,
        "quota_allotted": allotted,
        "quota_remaining": allotted - used,
        "quota_percent_used": round(percent_used, 2),
        "quota_percent_total_limit": sf.quota_percent_total,
        "is_near_limit": percent_used >= sf.quota_percent_total,
        **_dd_metric(f"mdi.salesforce.api.{phase}", round(percent_used, 2)),
    }

    LOGGER.warning(json.dumps(log_data))


def log_quota_consumed(
    sf: "Salesforce",
    pre_used: int,
    post_used: int,
    allotted: int,
) -> None:
    """Log the number of API calls consumed by this extraction job.

    Args:
        sf: Salesforce client instance
        pre_used: API calls used before extraction
        post_used: API calls used after extraction
        allotted: Total API calls allotted
    """
    calls_consumed = post_used - pre_used
    percent_consumed = (calls_consumed / allotted * 100) if allotted > 0 else 0

    log_data = {
        "event_type": "salesforce_quota_consumed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tenant_id": get_tenant_id(),
        "api_type": sf.api_type,
        "calls_consumed": calls_consumed,
        "percent_consumed": round(percent_consumed, 2),
        "pre_used": pre_used,
        "post_used": post_used,
        "allotted": allotted,
        **_dd_metric("mdi.salesforce.api.calls_consumed", calls_consumed),
    }

    LOGGER.warning(json.dumps(log_data))


def log_stream_sync_start(
    stream_name: str,
    replication_method: str | None = None,
) -> None:
    """Log when a specific stream sync starts.

    Args:
        stream_name: Name of the stream being synced
        replication_method: INCREMENTAL or FULL_TABLE
    """
    log_data = {
        "event_type": "salesforce_stream_start",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tenant_id": get_tenant_id(),
        "stream_name": stream_name,
        "replication_method": replication_method,
    }

    LOGGER.warning(json.dumps(log_data))


def log_stream_sync_complete(
    stream_name: str,
    records_count: int | None = None,
    success: bool = True,
) -> None:
    """Log when a specific stream sync completes.

    Args:
        stream_name: Name of the stream that was synced
        records_count: Number of records synced
        success: Whether the sync was successful
    """
    log_data = {
        "event_type": "salesforce_stream_complete",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tenant_id": get_tenant_id(),
        "stream_name": stream_name,
        "records_count": records_count,
        "success": success,
        **_dd_metric("mdi.salesforce.stream.records", records_count or 0, "count"),
    }

    LOGGER.warning(json.dumps(log_data))
