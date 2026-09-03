"""Unit tests for the per-object SOQL row filter.

The legacy puller appends a tenant-configured predicate to its SELECT so the excluded
records are never read out of the CRM. The tap never received the value, so a tenant
moving onto this extract path silently began ingesting the cohort its filter excludes.

Covered here:
- the filter lands between the date window and ORDER BY, on every caller's query shape
- a stream with no replication key gets its own WHERE rather than a dangling AND
- windowed streams keep their start-date floor when a filter is applied
- soft-deleted rows are exempted, so a deletion still reaches us after a record
  leaves the filter's cohort -- except on Task, which excludes them on purpose
- an empty filtered window advances the bookmark instead of freezing the cursor
- a filter that should never have got this far stops the run rather than being dropped

Run with:
    pytest tests/test_row_filter.py -v
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from singer import metadata as singer_metadata
from singer import metrics
from singer import utils as singer_utils

from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.exceptions import TapSalesforceExceptionError
from tap_salesforce.sync import sync_records

# Verbatim from the three tenants that carry a filter today, as re-emitted by the
# config service: one parenthesised group, canonical keywords.
FILTER_LEADSOURCE = "(leadsource != 'Zoominfo' AND leadsource != 'Apollo')"
FILTER_RECORD_TYPE = "(RecordTypeId IN ('01280000000Hn2LAAS', '0128000000046qhAAA'))"


def emitted(conditions, soft_deletes=True):
    """The predicate the tap should emit for a given configured filter.

    The tap re-wraps whatever it is handed rather than trusting the shape of a value it
    did not emit, so a predicate that already arrives parenthesised gains one more pair.
    """
    inner = f"({conditions})"
    return f"({inner} OR IsDeleted = true)" if soft_deletes else inner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_sf(objects_config=None, default_start_date="2024-01-01T00:00:00Z",
            api_type="REST", api_type_overrides=None):
    """Build a Salesforce instance without network calls."""
    sf = Salesforce.__new__(Salesforce)
    sf.windowed_objects = Salesforce.WINDOWED_OBJECTS
    sf.limit_windowed_objects_month = None
    sf.pull_config_objects = objects_config
    sf.select_fields_by_default = True
    sf.api_type = api_type
    sf.api_type_overrides = dict(api_type_overrides or {})
    sf._pk_chunking_streams = set()
    sf.default_start_date = singer_utils.strptime_to_utc(default_start_date).isoformat()
    return sf


def make_catalog_entry(stream, replication_key="SystemModstamp", fields=("Id", "IsDeleted", "SystemModstamp")):
    """Build a Singer catalog entry whose fields are all selected."""
    mdata = singer_metadata.new()
    if replication_key:
        singer_metadata.write(mdata, (), "replication-key", replication_key)
    for field in fields:
        singer_metadata.write(mdata, ("properties", field), "inclusion", "available")
        singer_metadata.write(mdata, ("properties", field), "selected", True)
    return {
        "stream": stream,
        "tap_stream_id": stream,
        "metadata": singer_metadata.to_list(mdata),
        "schema": {"properties": {field: {"type": ["null", "string"]} for field in fields}},
    }


def objects_config(name, conditions=None, columns=("Id",)):
    """Build the objects config the ingestion config service publishes."""
    entry = {"name": name, "columns": list(columns)}
    if conditions is not None:
        entry["conditions"] = conditions
    return [entry]


# ---------------------------------------------------------------------------
# Reading the filter out of the pull config
# ---------------------------------------------------------------------------

class TestParseConditions:
    """The filter has to survive the same config shapes `columns` already handles."""

    def test_conditions_are_read_from_a_list(self):
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        assert sf._parse_object_conditions(sf.pull_config_objects, "Lead") == FILTER_LEADSOURCE

    def test_conditions_are_read_from_a_json_string(self):
        """mk-airflow writes the config to S3 as JSON, so the tap can receive a string."""
        sf = make_sf(json.dumps(objects_config("Lead", FILTER_LEADSOURCE)))
        assert sf._parse_object_conditions(sf.pull_config_objects, "Lead") == FILTER_LEADSOURCE

    def test_stream_matching_stays_case_insensitive(self):
        sf = make_sf(objects_config("lead", FILTER_LEADSOURCE))
        assert sf._parse_object_conditions(sf.pull_config_objects, "Lead") == FILTER_LEADSOURCE

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_absent_or_blank_conditions_read_as_none(self, value):
        """Blank must mean "no filter", not "a predicate that admits nothing"."""
        sf = make_sf(objects_config("Lead", value))
        assert sf._parse_object_conditions(sf.pull_config_objects, "Lead") is None

    def test_columns_still_parse(self):
        """The refactor that added conditions must not change what `columns` returns."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE, columns=("Id", "Email")))
        assert sf._parse_objects_config(sf.pull_config_objects, "Lead") == ["Id", "Email"]

    def test_malformed_config_is_still_a_warning_not_a_crash(self):
        """Unchanged behaviour: a broken config degrades, it does not raise."""
        sf = make_sf("{not json")
        assert sf._parse_objects_config(sf.pull_config_objects, "Lead") == []
        assert sf._parse_object_conditions(sf.pull_config_objects, "Lead") is None


# ---------------------------------------------------------------------------
# Query construction
# ---------------------------------------------------------------------------

class TestQueryConstruction:
    """Where the predicate lands in the emitted SOQL."""

    def test_filter_sits_between_the_date_window_and_order_by(self):
        """Appending past the ORDER BY would be a syntax error."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        query = sf._build_query_string(
            make_catalog_entry("Lead"), "2024-01-01T00:00:00Z", end_date="2024-02-01T00:00:00Z"
        )

        assert query.index("SystemModstamp <") < query.index(FILTER_LEADSOURCE)
        assert query.index(FILTER_LEADSOURCE) < query.index("ORDER BY")

    def test_filter_is_anded_onto_the_replication_key_window(self):
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        query = sf._build_query_string(make_catalog_entry("Lead"), "2024-01-01T00:00:00Z")

        assert " WHERE SystemModstamp >= 2024-01-01T00:00:00Z" in query
        assert f" AND {emitted(FILTER_LEADSOURCE)}" in query

    def test_no_filter_leaves_the_query_untouched(self):
        """A stream with no filter must emit exactly what it emitted before."""
        entry = make_catalog_entry("Lead")
        with_config = make_sf(objects_config("Lead"))._build_query_string(entry, "2024-01-01T00:00:00Z")
        without_config = make_sf(None)._build_query_string(entry, "2024-01-01T00:00:00Z")

        assert with_config == without_config
        assert "WHERE SystemModstamp >= 2024-01-01T00:00:00Z" in with_config
        assert "AND (" not in with_config

    def test_stream_without_a_replication_key_gets_its_own_where(self):
        """Full-table streams reach the arm that has no WHERE clause to extend."""
        sf = make_sf(objects_config("Account", FILTER_RECORD_TYPE))
        query = sf._build_query_string(make_catalog_entry("Account", replication_key=None), "2024-01-01T00:00:00Z")

        assert query.count("WHERE") == 1
        assert query.endswith(f"WHERE {emitted(FILTER_RECORD_TYPE)}")
        assert " AND (" not in query

    def test_stream_without_a_replication_key_and_no_filter_is_unchanged(self):
        sf = make_sf(None)
        query = sf._build_query_string(make_catalog_entry("Account", replication_key=None), "2024-01-01T00:00:00Z")

        assert "WHERE" not in query

    def test_filter_survives_order_by_clause_false(self):
        """The Bulk2 path suppresses ORDER BY; the filter still has to be there."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        query = sf._build_query_string(
            make_catalog_entry("Lead"), "2024-01-01T00:00:00Z", order_by_clause=False
        )

        assert "ORDER BY" not in query
        assert FILTER_LEADSOURCE in query
        assert query.rstrip().endswith(")")

    def test_windowed_stream_keeps_its_start_date_floor(self):
        """The month limit arrives as the start_date, so the filter ANDs onto it.

        Asserted rather than assumed: the floor and the filter share one predicate, and
        a filter that displaced the floor would widen a deliberately narrowed window.
        """
        sf = make_sf(objects_config("Task", FILTER_LEADSOURCE))
        sf.limit_windowed_objects_month = 9
        entry = make_catalog_entry("Task")

        floor = sf.get_start_date({}, entry)
        query = sf._build_query_string(entry, floor)

        assert f"WHERE SystemModstamp >= {floor}" in query
        assert FILTER_LEADSOURCE in query
        # The floor is recent, not the tap's configured start date.
        assert floor > "2024-06-01"


# ---------------------------------------------------------------------------
# Soft deletes
# ---------------------------------------------------------------------------

class TestSoftDeleteExemption:
    """A deletion must still arrive after a record leaves the filter's cohort."""

    def test_is_deleted_is_exempted_when_the_object_has_the_field(self):
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        assert sf.get_row_filter(make_catalog_entry("Lead")) == emitted(FILTER_LEADSOURCE)

    def test_no_exemption_when_the_object_has_no_is_deleted_field(self):
        """Referencing a field the object does not have would fail the whole query."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        entry = make_catalog_entry("Lead", fields=("Id", "SystemModstamp"))

        assert sf.get_row_filter(entry) == emitted(FILTER_LEADSOURCE, soft_deletes=False)

    def test_legitimate_value_containing_a_reserved_word_is_not_rejected(self):
        """A company really can be called "Select Comfort", and that is data, not syntax.

        The guard looks at the predicate's syntax with quoted values blanked out, so a
        filter the config service already approved is not rejected here for the
        contents of one of its literals.
        """
        sf = make_sf(objects_config("Lead", "(Company = 'Select Financial Group')"))

        assert sf.get_row_filter(make_catalog_entry("Lead")) == emitted("(Company = 'Select Financial Group')")

    @pytest.mark.parametrize(
        "value",
        ["Smith--Jones", "a; b", "Select Comfort", "x /* y"],
    )
    def test_forbidden_tokens_inside_a_literal_are_data(self, value):
        """Each forbidden token is only dangerous outside a quoted value."""
        sf = make_sf(objects_config("Lead", f"(LastName = '{value}')"))

        assert value in sf.get_row_filter(make_catalog_entry("Lead"))

    def test_task_is_not_exempted(self):
        """Task is queried through the endpoint that hides soft-deleted rows on purpose.

        Its deleted Activity records are excluded so they stay out of the
        distinct-who/what ceiling that makes Task queries fail. Re-admitting them
        through the filter would work against that.
        """
        sf = make_sf(objects_config("Task", FILTER_LEADSOURCE))

        assert sf.get_row_filter(make_catalog_entry("Task")) == emitted(FILTER_LEADSOURCE, soft_deletes=False)

    def test_task_on_bulk2_is_exempted(self):
        """Bulk2 has no Task special case and always queries soft-deleted rows too.

        The exemption follows what the transport will actually do, so routing Task to
        Bulk2 puts it back on the normal path rather than leaving its deletions stuck.
        """
        sf = make_sf(objects_config("Task", FILTER_LEADSOURCE), api_type_overrides={"task": "BULK2"})

        assert sf.get_row_filter(make_catalog_entry("Task")) == emitted(FILTER_LEADSOURCE)

    def test_field_lookup_is_case_exact(self):
        """Schema keys come straight from the describe response and are never normalised."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        entry = make_catalog_entry("Lead", fields=("Id", "isdeleted", "SystemModstamp"))

        assert sf.get_row_filter(entry) == emitted(FILTER_LEADSOURCE, soft_deletes=False)


# ---------------------------------------------------------------------------
# Defence in depth
# ---------------------------------------------------------------------------

class TestFilterGuard:
    """A value that should never reach the tap stops the run rather than being ignored."""

    @pytest.mark.parametrize(
        "bad_filter",
        [
            "(a = 1); DROP TABLE x",
            "(a = 1) -- comment",
            "(a = 1) /* comment */",
            "Id IN (SELECT AccountId FROM Opportunity)",
        ],
    )
    def test_forbidden_tokens_raise(self, bad_filter):
        sf = make_sf(objects_config("Lead", bad_filter))

        with pytest.raises(TapSalesforceExceptionError):
            sf.get_row_filter(make_catalog_entry("Lead"))

    def test_over_length_filter_raises(self):
        sf = make_sf(objects_config("Lead", "(" + "a = 1 AND " * 200 + "b = 1)"))

        with pytest.raises(TapSalesforceExceptionError, match="over the 1000 character limit"):
            sf.get_row_filter(make_catalog_entry("Lead"))

    def test_the_run_stops_rather_than_extracting_unfiltered(self):
        """The whole point: a broken filter must not become "no filter"."""
        sf = make_sf(objects_config("Lead", "(a = 1); DROP TABLE x"))

        with pytest.raises(TapSalesforceExceptionError):
            sf._build_query_string(make_catalog_entry("Lead"), "2024-01-01T00:00:00Z")


# ---------------------------------------------------------------------------
# The cursor guard
# ---------------------------------------------------------------------------

class TestEmptyFilteredWindow:
    """The failure with no legacy counterpart: a zero-row window freezing the cursor."""

    @staticmethod
    def _sync(sf, entry, records, state):
        with metrics.record_counter(entry["stream"]) as counter, \
                patch.object(Salesforce, "query", return_value=iter(records), create=True), \
                patch("tap_salesforce.sync.tap_output.write_record"), \
                patch("tap_salesforce.sync.tap_output.write_state"), \
                patch("tap_salesforce.sync.tap_output.write_message"):
            sync_records(sf, entry, state, counter, state_msg_threshold=1000)
        return state

    def test_empty_filtered_window_advances_the_bookmark(self):
        """Without this the next run re-reads the same range, one cycle wider each time."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        entry = make_catalog_entry("Lead")
        state = {"bookmarks": {"Lead": {"SystemModstamp": "2024-01-01T00:00:00.000000Z"}}}

        before = singer_utils.now()
        state = self._sync(sf, entry, [], state)
        after = singer_utils.now()

        advanced = state["bookmarks"]["Lead"]["SystemModstamp"]
        assert before <= singer_utils.strptime_with_tz(advanced) <= after

    def test_empty_unfiltered_window_leaves_the_bookmark_alone(self):
        """An unfiltered empty window means nothing changed, and must behave as before."""
        sf = make_sf(objects_config("Lead"))
        entry = make_catalog_entry("Lead")
        state = {"bookmarks": {"Lead": {"SystemModstamp": "2024-01-01T00:00:00.000000Z"}}}

        state = self._sync(sf, entry, [], state)

        assert state["bookmarks"]["Lead"]["SystemModstamp"] == "2024-01-01T00:00:00.000000Z"

    def test_returned_records_still_set_the_bookmark(self):
        """The guard must not override a bookmark a real record produced."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        entry = make_catalog_entry("Lead")
        state = {"bookmarks": {"Lead": {"SystemModstamp": "2024-01-01T00:00:00.000000Z"}}}

        record = {"Id": "00Q1", "IsDeleted": False, "SystemModstamp": "2024-03-05T10:00:00.000000Z"}
        state = self._sync(sf, entry, [record], state)

        assert state["bookmarks"]["Lead"]["SystemModstamp"] == "2024-03-05T10:00:00.000000Z"

    def test_pk_chunked_empty_filtered_window_also_advances(self):
        """The chunked path writes one bookmark at the end; it has to advance too."""
        sf = make_sf(objects_config("Lead", FILTER_LEADSOURCE))
        entry = make_catalog_entry("Lead")
        sf.mark_pk_chunking("Lead")
        state = {"bookmarks": {"Lead": {"SystemModstamp": "2024-01-01T00:00:00.000000Z"}}}

        before = singer_utils.now()
        state = self._sync(sf, entry, [], state)

        advanced = state["bookmarks"]["Lead"]["SystemModstamp"]
        assert singer_utils.strptime_with_tz(advanced) >= before


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

class TestErrorBodyHandling:
    """A non-JSON error body must not bury the error that actually happened."""

    def test_non_json_error_body_surfaces_the_http_error(self):
        """An over-long request line returns HTML from the proxy, not a Salesforce error."""
        from requests.exceptions import HTTPError

        from tap_salesforce.salesforce.rest import Rest

        response = MagicMock()
        response.status_code = 431
        response.content = b"<html>Request Header Fields Too Large</html>"
        response.json.side_effect = ValueError("no json")

        sf = make_sf(None)
        sf.auth = MagicMock(rest_headers={})

        rest = Rest(sf)
        entry = make_catalog_entry("Lead")

        with patch.object(Rest, "_sync_records", side_effect=HTTPError(response=response)), \
                pytest.raises(HTTPError):
            list(rest._query_recur("SELECT Id FROM Lead", entry, "2024-01-01T00:00:00Z"))


# ---------------------------------------------------------------------------
# Bulk job lifecycle
# ---------------------------------------------------------------------------

class TestBulkJobOrdering:
    """An unusable filter must fail before a Bulk job exists on Salesforce's side.

    The Bulk v1 path does not build its query until _add_batch, which runs after
    _create_job. Validating there would raise with a job already open and nothing left
    to close it -- once per scheduled run, until the org's job quota is gone.
    """

    @staticmethod
    def _bulk_for(conditions):
        from tap_salesforce.salesforce.bulk import Bulk

        sf = make_sf(objects_config("Contact", conditions))
        sf.jobs_completed = 0
        return Bulk(sf), make_catalog_entry("Contact")

    def test_invalid_filter_raises_before_the_job_is_created(self):
        from tap_salesforce.salesforce.bulk import Bulk

        bulk, entry = self._bulk_for("(LastName != 'x'); DROP TABLE y")

        with patch.object(Bulk, "_create_job") as create_job, \
                patch.object(Bulk, "_add_batch") as add_batch, \
                pytest.raises(TapSalesforceExceptionError, match="not a valid predicate"):
            list(bulk._bulk_query(entry, {}))

        create_job.assert_not_called()
        add_batch.assert_not_called()

    def test_a_usable_filter_still_creates_the_job(self):
        """The guard must not become a gate that blocks the normal path."""
        from tap_salesforce.salesforce.bulk import Bulk

        bulk, entry = self._bulk_for(FILTER_LEADSOURCE)

        with patch.object(Bulk, "_create_job", return_value="JOB1") as create_job, \
                patch.object(Bulk, "_add_batch", return_value="BATCH1"), \
                patch.object(Bulk, "_close_job"), \
                patch.object(Bulk, "_poll_on_batch_status", return_value={"state": "Completed"}), \
                patch.object(Bulk, "get_batch_results", return_value=iter([])):
            list(bulk._bulk_query(entry, {}))

        create_job.assert_called_once()


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

class TestErrorCodeExtraction:
    """`MALFORMED_QUERY` is the error a rejected predicate produces, so it has a name."""

    def test_malformed_query_is_extracted_from_a_salesforce_error_body(self):
        from tap_salesforce.salesforce.rest import _error_code

        response = MagicMock()
        response.json.return_value = [{"errorCode": "MALFORMED_QUERY", "message": "unexpected token: 'OR'"}]

        assert _error_code(response, "Lead") == "MALFORMED_QUERY"

    def test_non_json_body_yields_no_error_code(self):
        from tap_salesforce.salesforce.rest import _error_code

        response = MagicMock()
        response.status_code = 431
        response.content = b"<html>Request Header Fields Too Large</html>"
        response.json.side_effect = ValueError("no json")

        assert _error_code(response, "Lead") is None

    def test_malformed_query_is_not_retried_or_bisected(self):
        """A malformed query is wrong, not too big, so bisecting it would loop."""
        from requests.exceptions import HTTPError

        from tap_salesforce.salesforce.rest import Rest

        response = MagicMock()
        response.json.return_value = [{"errorCode": "MALFORMED_QUERY", "message": "unexpected token: 'OR'"}]

        sf = make_sf(None)
        sf.auth = MagicMock(rest_headers={})
        rest = Rest(sf)

        with patch.object(Rest, "_sync_records", side_effect=HTTPError(response=response)) as sync_records_mock, \
                pytest.raises(HTTPError):
            list(rest._query_recur("SELECT Id FROM Lead", make_catalog_entry("Lead"), "2024-01-01T00:00:00Z"))

        assert sync_records_mock.call_count == 1
