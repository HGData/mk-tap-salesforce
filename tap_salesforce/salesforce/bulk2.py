import csv
import io
import json
import sys
import time

import singer
from singer import metrics

# Optimized polling interval to reduce API calls
BATCH_STATUS_POLLING_SLEEP = 30  # Increased from 20 to reduce polling frequency
DEFAULT_CHUNK_SIZE = 50000

LOGGER = singer.get_logger()


class Bulk2:
    bulk_url = "{}/services/data/v60.0/jobs/query"

    def __init__(self, sf):
        csv.field_size_limit(sys.maxsize)
        self.sf = sf

    def query(self, catalog_entry, state):
        job_id = self._create_job(catalog_entry, state)
        self._wait_for_job(job_id)

        for batch in self._get_next_batch(job_id):
            yield from csv.DictReader(io.StringIO(batch.decode("utf-8").replace("\0", "")))

    def _get_bulk_headers(self):
        return {**self.sf.auth.rest_headers, "Content-Type": "application/json"}

    def _create_job(self, catalog_entry, state):
        url = self.bulk_url.format(self.sf.instance_url)
        start_date = self.sf.get_start_date(state, catalog_entry)

        query = self.sf._build_query_string(catalog_entry, start_date, order_by_clause=False)

        body = {
            "operation": "queryAll",
            "query": query,
        }

        with metrics.http_request_timer("create_job") as timer:
            timer.tags["sobject"] = catalog_entry["stream"]
            resp = self.sf._make_request("POST", url, headers=self._get_bulk_headers(), body=json.dumps(body))

        job = resp.json()

        return job["id"]

    def _wait_for_job(self, job_id):
        status_url = self.bulk_url + "/{}"
        url = status_url.format(self.sf.instance_url, job_id)
        status = None
        # Use exponential backoff for polling to reduce API calls
        sleep_time = BATCH_STATUS_POLLING_SLEEP
        max_sleep_time = 120  # Cap at 2 minutes

        while status not in ("JobComplete", "Failed"):
            resp = self.sf._make_request("GET", url, headers=self._get_bulk_headers()).json()
            status = resp["state"]

            if status == "JobComplete":
                break

            if status == "Failed":
                raise Exception(f"Job failed: {resp.json()}")

            time.sleep(sleep_time)
            # Increase sleep time exponentially, but cap it
            sleep_time = min(sleep_time * 1.2, max_sleep_time)

    def _get_next_batch(self, job_id):
        url = self.bulk_url + "/{}/results"
        url = url.format(self.sf.instance_url, job_id)
        locator = ""

        while locator != "null":
            params = {"maxRecords": DEFAULT_CHUNK_SIZE}

            if locator != "":
                params["locator"] = locator

            resp = self.sf._make_request("GET", url, headers=self._get_bulk_headers(), params=params)
            locator = resp.headers.get("Sforce-Locator")

            yield resp.content
