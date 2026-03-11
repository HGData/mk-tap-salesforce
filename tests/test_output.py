import datetime
import io
import json
import logging
import threading
from unittest.mock import patch

import pytest

from tap_salesforce import output as tap_output


class TestConcurrentWrites:
    """Bug 1 fix — thread-safe stdout lock."""

    def test_concurrent_writes_produce_valid_jsonl(self):
        """8 threads × 1000 records — every line must be valid JSON, no interleaving."""
        output = io.StringIO()
        errors = []

        def worker(stream_name):
            for i in range(1000):
                try:
                    tap_output.write_record(
                        stream_name,
                        # long value stresses the pipe buffer (> 4KB triggers the race without the lock)
                        {"id": i, "data": f"{stream_name}-record-{i}-" + "x" * 200},
                    )
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=worker, args=(f"Stream{i}",)) for i in range(8)]

        with patch("sys.stdout", output):
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert not errors, f"Worker threads raised: {errors}"

        lines = [l for l in output.getvalue().splitlines() if l.strip()]
        assert len(lines) == 8000, f"Expected 8000 lines, got {len(lines)}"

        for line in lines:
            msg = json.loads(line)  # raises JSONDecodeError if bytes were interleaved
            assert msg["type"] == "RECORD"
            assert msg["stream"].startswith("Stream")


class TestSafeBookmarkValue:
    """Bug 2 fix — future bookmark guard."""

    def test_past_value_passes_through(self):
        value = "2026-01-01T00:00:00+00:00"
        assert tap_output.safe_bookmark_value("Contact", "SystemModstamp", value) == value

    def test_near_future_within_threshold_passes_through(self):
        # 30 minutes ahead — inside the default 3600s window, must not be capped
        near_future = (
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=30)
        ).isoformat()
        result = tap_output.safe_bookmark_value("Contact", "SystemModstamp", near_future)
        assert result == near_future

    def test_far_future_is_capped_at_now(self):
        result = tap_output.safe_bookmark_value(
            "Contact", "SystemModstamp", "2099-01-01T00:00:00+00:00"
        )
        result_dt = datetime.datetime.fromisoformat(result)
        now = datetime.datetime.now(datetime.timezone.utc)
        assert result_dt <= now + datetime.timedelta(seconds=5)

    def test_z_suffix_handled(self):
        # Exact format Salesforce returns — Z instead of +00:00
        result = tap_output.safe_bookmark_value(
            "Contact", "SystemModstamp", "2099-01-01T00:00:00Z"
        )
        result_dt = datetime.datetime.fromisoformat(result)
        now = datetime.datetime.now(datetime.timezone.utc)
        assert result_dt <= now + datetime.timedelta(seconds=5)

    def test_none_passes_through(self):
        assert tap_output.safe_bookmark_value("Contact", "SystemModstamp", None) is None

    def test_critical_log_emitted_on_cap(self, caplog):
        with caplog.at_level(logging.CRITICAL, logger="tap_salesforce.output"):
            tap_output.safe_bookmark_value(
                "Contact", "SystemModstamp", "2099-01-01T00:00:00Z"
            )
        assert any("BOOKMARK_GUARD" in r.message for r in caplog.records)
