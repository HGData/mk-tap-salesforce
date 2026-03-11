import datetime
import io
import json
import logging
import sys
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
        write_counts = {}

        def worker(stream_name):
            write_counts[stream_name] = 0
            for i in range(1000):
                try:
                    tap_output.write_record(
                        stream_name,
                        {"id": i, "data": f"{stream_name}-record-{i}-" + "x" * 200},
                    )
                    write_counts[stream_name] += 1
                except Exception as e:
                    errors.append(e)
        #    print(f"  [Thread] {stream_name} finished — wrote {write_counts[stream_name]} records")
            print(f"  [Thread] {stream_name} finished — wrote {write_counts[stream_name]} records", file=sys.stderr)

        threads = [threading.Thread(target=worker, args=(f"Stream{i}",)) for i in range(8)]

        print("\n--- TestConcurrentWrites: starting 8 threads ---")
        with patch("sys.stdout", output):
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        print(f"  All threads done. Errors: {len(errors)}")
        assert not errors, f"Worker threads raised: {errors}"

        raw = output.getvalue()
        lines = [l for l in raw.splitlines() if l.strip()]
        print(f"  Total lines captured: {len(lines)} (expected 8000)")
        assert len(lines) == 8000, f"Expected 8000 lines, got {len(lines)}"

        corrupt = 0
        stream_counts = {}
        for i, line in enumerate(lines):
            msg = json.loads(line)   # raises JSONDecodeError if bytes were interleaved
            assert msg["type"] == "RECORD"
            assert msg["stream"].startswith("Stream")
            stream_counts[msg["stream"]] = stream_counts.get(msg["stream"], 0) + 1

        print("  Per-stream line counts:")
        for stream, count in sorted(stream_counts.items()):
            print(f"    {stream}: {count} lines")

        print("  All 8000 lines are valid JSON — no interleaving detected ✓")


class TestSafeBookmarkValue:
    """Bug 2 fix — future bookmark guard."""

    def test_past_value_passes_through(self):
        value = "2026-01-01T00:00:00+00:00"
        print(f"\n--- test_past_value_passes_through ---")
        print(f"  Input:  {value}")
        result = tap_output.safe_bookmark_value("Contact", "SystemModstamp", value)
        print(f"  Output: {result}")
        print(f"  Changed: {result != value}")
        assert result == value

    def test_near_future_within_threshold_passes_through(self):
        near_future = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)
        ).isoformat()
        print(f"\n--- test_near_future_within_threshold_passes_through ---")
        print(f"  Input:  {near_future}  (+30 min, within 3600s threshold)")
        result = tap_output.safe_bookmark_value("Contact", "SystemModstamp", near_future)
        print(f"  Output: {result}")
        print(f"  Passed through unchanged: {result == near_future}")
        assert result == near_future

    def test_far_future_is_capped_at_now(self):
        far_future = "2099-01-01T00:00:00+00:00"
        print(f"\n--- test_far_future_is_capped_at_now ---")
        print(f"  Input:  {far_future}")
        result = tap_output.safe_bookmark_value("Contact", "SystemModstamp", far_future)
        now = datetime.datetime.now(datetime.timezone.utc)
        result_dt = datetime.datetime.fromisoformat(result)
        print(f"  Output: {result}")
        print(f"  Now:    {now.isoformat()}")
        print(f"  Capped (output <= now): {result_dt <= now + datetime.timedelta(seconds=5)}")
        assert result_dt <= now + datetime.timedelta(seconds=5)

    def test_z_suffix_handled(self):
        far_future = "2099-01-01T00:00:00Z"
        print(f"\n--- test_z_suffix_handled ---")
        print(f"  Input:  {far_future}  (Z suffix as Salesforce sends it)")
        result = tap_output.safe_bookmark_value("Contact", "SystemModstamp", far_future)
        now = datetime.datetime.now(datetime.timezone.utc)
        result_dt = datetime.datetime.fromisoformat(result)
        print(f"  Output: {result}")
        print(f"  Capped (output <= now): {result_dt <= now + datetime.timedelta(seconds=5)}")
        assert result_dt <= now + datetime.timedelta(seconds=5)

    def test_none_passes_through(self):
        print(f"\n--- test_none_passes_through ---")
        print(f"  Input:  None")
        result = tap_output.safe_bookmark_value("Contact", "SystemModstamp", None)
        print(f"  Output: {result}")
        print(f"  Is None: {result is None}")
        assert result is None

    def test_critical_log_emitted_on_cap(self, caplog):
        far_future = "2099-01-01T00:00:00Z"
        print(f"\n--- test_critical_log_emitted_on_cap ---")
        print(f"  Input:  {far_future}")
        with caplog.at_level(logging.CRITICAL, logger="tap_salesforce.output"):
            tap_output.safe_bookmark_value("Contact", "SystemModstamp", far_future)
        matching = [r for r in caplog.records if "BOOKMARK_GUARD" in r.message]
        print(f"  CRITICAL logs captured: {len(matching)}")
        for r in matching:
            print(f"  Log: {r.message}")
        assert len(matching) == 1
