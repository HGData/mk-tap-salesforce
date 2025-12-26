import json
import re
import time
from datetime import timedelta

import backoff
import requests
import singer
import singer.utils as singer_utils
from singer import metadata, metrics

from tap_salesforce.salesforce.bulk import Bulk
from tap_salesforce.salesforce.bulk2 import Bulk2
from tap_salesforce.salesforce.credentials import SalesforceAuth
from tap_salesforce.salesforce.exceptions import (
    SFDCCustomNotAcceptableError,
    TapSalesforceExceptionError,
    TapSalesforceQuotaExceededError,
)
from tap_salesforce.salesforce.rest import Rest

LOGGER = singer.get_logger()

# Cache describe results for 1 hour to reduce API calls during discovery
DESCRIBE_CACHE_TTL = 3600  # 1 hour in seconds

BULK_API_TYPE = "BULK"
BULK2_API_TYPE = "BULK2"
REST_API_TYPE = "REST"

STRING_TYPES = {
    "id",
    "string",
    "picklist",
    "textarea",
    "phone",
    "url",
    "reference",
    "multipicklist",
    "combobox",
    "encryptedstring",
    "email",
    "complexvalue",  # TODO: Unverified
    "masterrecord",
    "datacategorygroupreference",
    "base64",
}

NUMBER_TYPES = {"double", "currency", "percent"}

DATE_TYPES = {"datetime", "date"}

BINARY_TYPES = {"byte"}

LOOSE_TYPES = {
    "anyType",
    # A calculated field's type can be any of the supported
    # formula data types (see https://developer.salesforce.com/docs/#i1435527)
    "calculated",
}


# The following objects are not supported by the bulk API.
UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS = {
    "AssetTokenEvent",
    "AttachedContentNote",
    "EventWhoRelation",
    "QuoteTemplateRichTextData",
    "TaskWhoRelation",
    "SolutionStatus",
    "ContractStatus",
    "RecentlyViewed",
    "DeclinedEventRelation",
    "AcceptedEventRelation",
    "TaskStatus",
    "PartnerRole",
    "TaskPriority",
    "CaseStatus",
    "UndecidedEventRelation",
    "OrderStatus",
}

# The following objects have certain WHERE clause restrictions so we exclude them.
QUERY_RESTRICTED_SALESFORCE_OBJECTS = {
    "Announcement",
    "CollaborationGroupRecord",
    "Vote",
    "IdeaComment",
    "FieldDefinition",
    "PlatformAction",
    "UserEntityAccess",
    "RelationshipInfo",
    "ContentFolderMember",
    "ContentFolderItem",
    "SearchLayout",
    "SiteDetail",
    "EntityParticle",
    "OwnerChangeOptionInfo",
    "DataStatistics",
    "UserFieldAccess",
    "PicklistValueInfo",
    "RelationshipDomain",
    "FlexQueueItem",
    "NetworkUserHistoryRecent",
    "FieldHistoryArchive",
    "RecordActionHistory",
    "FlowVersionView",
    "FlowVariableView",
    "AppTabMember",
    "ColorDefinition",
    "IconDefinition",
}

# The following objects are not supported by the query method being used.
QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS = {
    "DataType",
    "ListViewChartInstance",
    "FeedLike",
    "OutgoingEmail",
    "OutgoingEmailRelation",
    "FeedSignal",
    "ActivityHistory",
    "EmailStatus",
    "UserRecordAccess",
    "Name",
    "AggregateResult",
    "OpenActivity",
    "ProcessInstanceHistory",
    "OwnedContentDocument",
    "FolderedContentDocument",
    "FeedTrackedChange",
    "CombinedAttachment",
    "AttachedContentDocument",
    "ContentBody",
    "NoteAndAttachment",
    "LookedUpFromActivity",
    "AttachedContentNote",
    "QuoteTemplateRichTextData",
}


def log_backoff_attempt(details):
    LOGGER.info("ConnectionError detected, triggering backoff: %d try", details.get("tries"))


def raise_for_status(resp):
    """
    Adds additional handling of HTTP Errors.

    `CustomNotAcceptable` is returned during discovery with status code 406.
        This error does not seem to be documented on Salesforce, and possibly
        is not the best error that Salesforce could return. It also appears
        that this error is ephemeral and resolved after retries.
    """
    if resp.status_code != 200:
        err_msg = f"{resp.status_code} Client Error: {resp.reason} for url: {resp.url}"
        LOGGER.warning(err_msg)

    if resp.status_code == 406 and "CustomNotAcceptable" in resp.reason:
        raise SFDCCustomNotAcceptableError(err_msg)
    else:
        resp.raise_for_status()


def field_to_property_schema(field, mdata):  # noqa: C901
    property_schema = {}

    field_name = field["name"]
    sf_type = field["type"]

    if sf_type in STRING_TYPES:
        property_schema["type"] = "string"
    elif sf_type in DATE_TYPES:
        date_type = {"type": "string", "format": "date-time"}
        string_type = {"type": ["string", "null"]}
        property_schema["anyOf"] = [date_type, string_type]
    elif sf_type == "boolean":
        property_schema["type"] = "boolean"
    elif sf_type in NUMBER_TYPES:
        property_schema["type"] = "number"
    elif sf_type == "address":
        property_schema["type"] = "object"
        property_schema["properties"] = {
            "street": {"type": ["null", "string"]},
            "state": {"type": ["null", "string"]},
            "postalCode": {"type": ["null", "string"]},
            "city": {"type": ["null", "string"]},
            "country": {"type": ["null", "string"]},
            "longitude": {"type": ["null", "number"]},
            "latitude": {"type": ["null", "number"]},
            "geocodeAccuracy": {"type": ["null", "string"]},
        }
    elif sf_type in ("int", "long"):
        property_schema["type"] = "integer"
    elif sf_type == "time":
        property_schema["type"] = "string"
    elif sf_type in LOOSE_TYPES:
        return property_schema, mdata  # No type = all types
    elif sf_type in BINARY_TYPES:
        mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "unsupported")
        mdata = metadata.write(mdata, ("properties", field_name), "unsupported-description", "binary data")
        return property_schema, mdata
    elif sf_type == "location":
        # geo coordinates are numbers or objects divided into two fields for lat/long
        property_schema["type"] = ["number", "object", "null"]
        property_schema["properties"] = {
            "longitude": {"type": ["null", "number"]},
            "latitude": {"type": ["null", "number"]},
        }
    elif sf_type == "json":
        property_schema["type"] = "string"
    else:
        raise TapSalesforceExceptionError(f"Found unsupported type: {sf_type}")

    # The nillable field cannot be trusted
    if field_name != "Id" and sf_type != "location" and sf_type not in DATE_TYPES:
        property_schema["type"] = ["null", property_schema["type"]]

    return property_schema, mdata


class Salesforce:
    # pylint: disable=too-many-instance-attributes,too-many-arguments
    def __init__(
        self,
        credentials=None,
        token=None,
        quota_percent_per_run=None,
        quota_percent_total=None,
        is_sandbox=None,
        select_fields_by_default=None,
        default_start_date=None,
        api_type=None,
        limit_tasks_month=None,
        pull_config_objects=None,
    ):
        self.api_type = api_type.upper() if api_type else None
        self.session = requests.Session()
        if isinstance(quota_percent_per_run, str) and quota_percent_per_run.strip() == "":
            quota_percent_per_run = None
        if isinstance(quota_percent_total, str) and quota_percent_total.strip() == "":
            quota_percent_total = None

        self.quota_percent_per_run = float(quota_percent_per_run) if quota_percent_per_run is not None else 25
        self.quota_percent_total = float(quota_percent_total) if quota_percent_total is not None else 80
        self.is_sandbox = is_sandbox is True or (isinstance(is_sandbox, str) and is_sandbox.lower() == "true")
        self.select_fields_by_default = select_fields_by_default is True or (
            isinstance(select_fields_by_default, str) and select_fields_by_default.lower() == "true"
        )
        self.rest_requests_attempted = 0
        self.jobs_completed = 0
        self.data_url = "{}/services/data/v60.0/{}"
        self.pk_chunking = False
        self.limit_tasks_month = limit_tasks_month
        self.pull_config_objects = pull_config_objects
        # Cache for describe calls to reduce API consumption
        self._describe_cache = {}
        self._describe_cache_timestamps = {}
        # Rate limiting: track last request time to throttle requests
        self._last_request_time = 0
        self._min_request_interval = 0.1  # Minimum 100ms between requests (10 req/sec max)
        # Track REST quota usage for this job (start and end values)
        self._initial_quota_used = None
        self._initial_quota_allotted = None
        self._final_quota_used = None
        self._final_quota_allotted = None
        self._max_quota_used_seen = 0

        self.auth = SalesforceAuth.from_credentials(credentials, is_sandbox=self.is_sandbox)

        # validate start_date
        self.default_start_date = (
            singer_utils.strptime_to_utc(default_start_date)
            if default_start_date
            else (singer_utils.now() - timedelta(weeks=4))
        ).isoformat()

        if default_start_date:
            LOGGER.info(
                "Parsed start date '%s' from value '%s'",
                self.default_start_date,
                default_start_date,
            )

    def _parse_objects_config(self, objects_config: str | list[dict], stream: str) -> list[str]:
        """Parse the OBJECTS configuration string into a list of fields for the given stream."""
        if not objects_config or not stream:
            return []

        try:
            objects_list = json.loads(objects_config) if isinstance(objects_config, str) else objects_config

            for obj in objects_list:
                if isinstance(obj, dict) and obj.get('name', '').lower() == stream.lower():
                    return obj.get('columns', [])
            return []
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            LOGGER.warning(f"Failed to parse OBJECTS configuration: {e}")
            return []

    def get_api_quota_info(self):
        """Fetch and return current API quota information from Salesforce.
        
        Note: This endpoint requires special permissions and may return 403 Forbidden.
        If it fails, we fall back to header-based quota tracking.
        """
        endpoint = "limits"
        url = self.data_url.format(self.instance_url, endpoint)

        try:
            resp = self._make_request("GET", url, headers=self.auth.rest_headers)
            quota_data = resp.json()

            quota_info = {}

            # REST API quota
            if "DailyApiRequests" in quota_data:
                rest_used = quota_data["DailyApiRequests"]["Max"] - quota_data["DailyApiRequests"]["Remaining"]
                rest_max = quota_data["DailyApiRequests"]["Max"]
                rest_remaining = quota_data["DailyApiRequests"]["Remaining"]
                rest_percent = (rest_used / rest_max * 100) if rest_max > 0 else 0
                quota_info["rest"] = {
                    "used": rest_used,
                    "max": rest_max,
                    "remaining": rest_remaining,
                    "percent_used": rest_percent,
                }
                
                # Capture initial quota if not already set (from limits endpoint)
                if self._initial_quota_used is None:
                    self._initial_quota_used = rest_used
                    self._initial_quota_allotted = rest_max
                    self._max_quota_used_seen = rest_used

            # Bulk API quota
            if "DailyBulkApiBatches" in quota_data:
                bulk_used = quota_data["DailyBulkApiBatches"]["Max"] - quota_data["DailyBulkApiBatches"]["Remaining"]
                bulk_max = quota_data["DailyBulkApiBatches"]["Max"]
                bulk_remaining = quota_data["DailyBulkApiBatches"]["Remaining"]
                bulk_percent = (bulk_used / bulk_max * 100) if bulk_max > 0 else 0
                quota_info["bulk"] = {
                    "used": bulk_used,
                    "max": bulk_max,
                    "remaining": bulk_remaining,
                    "percent_used": bulk_percent,
                }

            return quota_info
        except requests.exceptions.HTTPError as e:
            # 403 Forbidden is common - user doesn't have permission to access /limits endpoint
            # This is fine, we'll use header-based tracking instead
            if e.response and e.response.status_code == 403:
                LOGGER.debug("Cannot access /limits endpoint (403 Forbidden) - using header-based quota tracking instead")
            else:
                LOGGER.debug("Failed to fetch API quota information from /limits endpoint: %s", e)
            return None
        except Exception as e:
            LOGGER.debug("Failed to fetch API quota information: %s", e)
            return None

    def log_api_quota(self, context="Current"):
        """Log current API quota status."""
        quota_info = self.get_api_quota_info()

        if quota_info is None:
            return

        if "rest" in quota_info:
            rest = quota_info["rest"]
            LOGGER.info(
                "%s REST API Quota: %d/%d used (%.1f%%), %d remaining",
                context,
                rest["used"],
                rest["max"],
                rest["percent_used"],
                rest["remaining"],
            )

        if "bulk" in quota_info:
            bulk = quota_info["bulk"]
            LOGGER.info(
                "%s Bulk API Quota: %d/%d used (%.1f%%), %d remaining",
                context,
                bulk["used"],
                bulk["max"],
                bulk["percent_used"],
                bulk["remaining"],
            )
    
    def log_job_quota_usage(self):
        """Log how much API quota this job consumed from start to end."""
        # Always show internal counter (most accurate for this job)
        internal_count = self.rest_requests_attempted
        
        # Also show quota header tracking if available
        if self._initial_quota_used is not None and self._final_quota_used is not None:
            quota_used_by_job = self._final_quota_used - self._initial_quota_used
            LOGGER.info(
                "This job used %d REST API requests (internal count: %d, system-wide quota change: %d from %d to %d out of %d total)",
                internal_count,
                internal_count,
                quota_used_by_job,
                self._initial_quota_used,
                self._final_quota_used,
                self._final_quota_allotted,
            )
        else:
            # No quota header tracking available (might be using Bulk API only or header not present)
            LOGGER.info(
                "This job used %d REST API requests (internal count)",
                internal_count,
            )

    # pylint: disable=anomalous-backslash-in-string,line-too-long
    def check_rest_quota_usage(self, headers):
        match = re.search(r"^api-usage=(\d+)/(\d+)$", headers.get("Sforce-Limit-Info"))

        if match is None:
            return

        used, allotted = map(int, match.groups())
        
        # Track maximum quota used to get the most accurate end value
        # (since header reflects system-wide usage, not just this job)
        if used > self._max_quota_used_seen:
            self._max_quota_used_seen = used
        
        # Store initial quota on first check (if not already set)
        if self._initial_quota_used is None:
            self._initial_quota_used = used
            self._initial_quota_allotted = allotted
        
        # Always update final quota to track the latest value
        self._final_quota_used = self._max_quota_used_seen
        self._final_quota_allotted = allotted

        percent_used_from_total = (used / allotted) * 100
        max_requests_for_run = int((self.quota_percent_per_run * allotted) / 100)

        if percent_used_from_total > self.quota_percent_total:
            total_message = (
                "Salesforce has reported {}/{} ({:3.2f}%) total REST quota "
                + "used across all Salesforce Applications. Terminating "
                + "replication to not continue past configured percentage "
                + "of {}% total quota."
            ).format(used, allotted, percent_used_from_total, self.quota_percent_total)
            raise TapSalesforceQuotaExceededError(total_message)
        elif self.rest_requests_attempted > max_requests_for_run:
            partial_message = (
                "This replication job has made {} REST requests ({:3.2f}% of "
                + "total quota). Terminating replication due to allotted "
                + "quota of {}% per replication."
            ).format(
                self.rest_requests_attempted,
                (self.rest_requests_attempted / allotted) * 100,
                self.quota_percent_per_run,
            )
            raise TapSalesforceQuotaExceededError(partial_message)

    def login(self):
        self.auth.login()

    @property
    def instance_url(self):
        return self.auth.instance_url

    # pylint: disable=too-many-arguments
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, SFDCCustomNotAcceptableError),
        max_tries=10,
        factor=2,
        on_backoff=log_backoff_attempt,
    )
    def _make_request(self, http_method, url, headers=None, body=None, stream=False, params=None):
        # Rate limiting: ensure minimum time between requests
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        if time_since_last_request < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last_request
            time.sleep(sleep_time)

        # Use debug level for request logging to reduce log verbosity
        if http_method == "GET":
            LOGGER.debug("Making %s request to %s with params: %s", http_method, url, params)
            resp = self.session.get(url, headers=headers, stream=stream, params=params)
        elif http_method == "POST":
            LOGGER.debug("Making %s request to %s", http_method, url)
            resp = self.session.post(url, headers=headers, data=body)
        else:
            raise TapSalesforceExceptionError("Unsupported HTTP method")

        self._last_request_time = time.time()

        raise_for_status(resp)

        if resp.headers.get("Sforce-Limit-Info") is not None:
            self.rest_requests_attempted += 1
            self.check_rest_quota_usage(resp.headers)

        return resp

    def describe(self, sobject=None):
        """Describes all objects or a specific object with caching to reduce API calls"""
        current_time = time.time()
        cache_key = str(sobject) if sobject else "all_objects"

        # Check cache first
        if cache_key in self._describe_cache:
            cache_age = current_time - self._describe_cache_timestamps.get(cache_key, 0)
            if cache_age < DESCRIBE_CACHE_TTL:
                LOGGER.debug("Using cached describe result for %s (age: %.1fs)", cache_key, cache_age)
                return self._describe_cache[cache_key]
            else:
                # Cache expired, remove it
                LOGGER.debug("Cache expired for %s (age: %.1fs), fetching fresh data", cache_key, cache_age)
                del self._describe_cache[cache_key]
                del self._describe_cache_timestamps[cache_key]

        headers = self.auth.rest_headers
        instance_url = self.auth.instance_url
        body = None
        method = "GET"
        if sobject is None:
            endpoint = "sobjects"
            endpoint_tag = "sobjects"
            url = self.data_url.format(instance_url, endpoint)
        elif isinstance(sobject, list):
            batch_length = len(sobject)
            if batch_length > 25:
                raise TapSalesforceExceptionError(f"Composite limited to 25 sObjects per batch. ({batch_length}).")
            endpoint = "composite/batch"
            endpoint_tag = "CompositeBatch"
            url = self.data_url.format(instance_url, endpoint)
            method = "POST"
            headers["Content-Type"] = "application/json"
            composite_subrequests = []
            for obj in sobject:
                sub_endpoint = f"sobjects/{obj}/describe"
                sub_url = self.data_url.format("", sub_endpoint)
                subrequest = {"method": "GET", "url": sub_url}
                composite_subrequests.append(subrequest)
            body = json.dumps({"batchRequests": composite_subrequests})
        else:
            endpoint = f"sobjects/{sobject}/describe"
            endpoint_tag = sobject
            url = self.data_url.format(instance_url, endpoint)

        with metrics.http_request_timer("describe") as timer:
            timer.tags["endpoint"] = endpoint_tag
            resp = self._make_request(method, url, headers=headers, body=body)

        if isinstance(sobject, list):
            result = resp.json()["results"]
        else:
            result = resp.json()

        # Cache the result
        self._describe_cache[cache_key] = result
        self._describe_cache_timestamps[cache_key] = current_time
        LOGGER.debug("Cached describe result for %s", cache_key)

        return result

    # pylint: disable=no-self-use
    def _get_selected_properties(self, catalog_entry):
        mdata = metadata.to_map(catalog_entry["metadata"])
        properties = catalog_entry["schema"].get("properties", {})
        stream = catalog_entry["stream"]
        meltano_selected_fields = [
            k
            for k in properties
            if singer.should_sync_field(
                metadata.get(mdata, ("properties", k), "inclusion"),
                metadata.get(mdata, ("properties", k), "selected"),
                self.select_fields_by_default,
            )
        ]

        pull_config_fields = self._parse_objects_config(self.pull_config_objects, stream)

        # Limit to 500 fields total to prevent header size issues
        max_fields = 500
        if len(pull_config_fields) > max_fields:
            LOGGER.warning(
                f"""Pull config contains more than {max_fields} fields for {stream}.
                Only the first {max_fields} fields will be used."""
            )

        if len(meltano_selected_fields) > max_fields:
            # Limit fields to prevent "Request Header Fields Too Large" error
            # Prioritize essential fields first
            essential_fields = ["Id", "SystemModstamp", "CreatedDate", "LastModifiedDate"]
            priority_fields = [f for f in essential_fields if f in meltano_selected_fields]
            priority_fields.extend([f for f in pull_config_fields if f in meltano_selected_fields])
            priority_fields = list(set(priority_fields))
            mk_fields = [f for f in meltano_selected_fields if "mk_" in f and f not in priority_fields]
            other_fields = [f for f in meltano_selected_fields if f not in priority_fields + mk_fields]

            ingested_fields = priority_fields[:max_fields]
            ingested_fields += mk_fields[:max_fields-len(ingested_fields)]
            ingested_fields += other_fields[:max_fields-len(ingested_fields)]


            LOGGER.warning(
                f"Limiting {stream} fields from {len(meltano_selected_fields)} to {max_fields} "
                "to prevent header size issues"
            )

            LOGGER.info(f"Selected {len(ingested_fields)} fields for {stream}: {ingested_fields}")
            return ingested_fields
        return meltano_selected_fields

    def get_start_date(self, state, catalog_entry):
        """Get the start date for a stream, applying task month limit if configured."""
        catalog_metadata = metadata.to_map(catalog_entry["metadata"])
        replication_key = catalog_metadata.get((), {}).get("replication-key")

        # Get the bookmark date or default start date
        start_date = (
            singer.get_bookmark(state, catalog_entry["tap_stream_id"], replication_key) or self.default_start_date
        )

        # Apply task month limit if this is a Task stream and limit is configured
        if (
            catalog_entry["tap_stream_id"] == "Task"
            and self.limit_tasks_month is not None
            and self.limit_tasks_month > 0
        ):
            # Calculate the earliest allowed date (limit_tasks_month ago from now)
            now = singer_utils.now()
            month_limit_date = now - timedelta(days=31 * self.limit_tasks_month)
            month_limit_date_str = month_limit_date.isoformat()

            # Use the later of the two dates (bookmark/default vs month limit)
            if start_date < month_limit_date_str:
                LOGGER.info(
                    "Task stream limited to %d months. Using start date %s instead of %s",
                    self.limit_tasks_month,
                    month_limit_date_str,
                    start_date,
                )
                return month_limit_date_str

        return start_date

    def _build_query_string(self, catalog_entry, start_date, end_date=None, order_by_clause=True):
        selected_properties = self._get_selected_properties(catalog_entry)

        query = "SELECT {} FROM {}".format(",".join(selected_properties), catalog_entry["stream"])

        catalog_metadata = metadata.to_map(catalog_entry["metadata"])
        replication_key = catalog_metadata.get((), {}).get("replication-key")

        if replication_key:
            where_clause = f" WHERE {replication_key} >= {start_date} "
            end_date_clause = f" AND {replication_key} < {end_date}" if end_date else ""

            order_by = f" ORDER BY {replication_key} ASC"
            if order_by_clause:
                return query + where_clause + end_date_clause + order_by

            return query + where_clause + end_date_clause
        else:
            return query

    def query(self, catalog_entry, state):
        if self.api_type == BULK_API_TYPE:
            bulk = Bulk(self)
            return bulk.query(catalog_entry, state)
        elif self.api_type == BULK2_API_TYPE:
            bulk = Bulk2(self)
            return bulk.query(catalog_entry, state)
        elif self.api_type == REST_API_TYPE:
            rest = Rest(self)
            return rest.query(catalog_entry, state)
        else:
            raise TapSalesforceExceptionError(f"api_type should be REST or BULK was: {self.api_type}")

    def get_blacklisted_objects(self):
        if self.api_type in [BULK_API_TYPE, BULK2_API_TYPE]:
            return UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS.union(QUERY_RESTRICTED_SALESFORCE_OBJECTS).union(
                QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS
            )
        elif self.api_type == REST_API_TYPE:
            return QUERY_RESTRICTED_SALESFORCE_OBJECTS.union(QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS)
        else:
            raise TapSalesforceExceptionError(f"api_type should be REST or BULK was: {self.api_type}")

    # pylint: disable=line-too-long
    def get_blacklisted_fields(self):
        if self.api_type == BULK_API_TYPE or self.api_type == BULK2_API_TYPE:
            return {
                (
                    "EntityDefinition",
                    "RecordTypesSupported",
                ): "this field is unsupported by the Bulk API."
            }
        elif self.api_type == REST_API_TYPE:
            return {}
        else:
            raise TapSalesforceExceptionError(f"api_type should be REST or BULK was: {self.api_type}")
