"""Stream type classes for tap-sumologic."""

import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional

from tap_sumologic.client import SumoLogicStream


class SearchJobStream(SumoLogicStream):
    """Define dynamic stream for Search Job API queries."""

    def __init__(
        self,
        tap: Any,
        name: str,
        query_type: str,
        primary_keys: Optional[list] = None,
        replication_key: Optional[str] = None,
        schema: Optional[dict] = None,
        query: Optional[str] = None,
        by_receipt_time: Optional[bool] = None,
        auto_parsing_mode: Optional[str] = None,
        quantization: Optional[int] = None,
        rollup: Optional[str] = None,
        timeshift: Optional[int] = None,
    ) -> None:
        """Class initialization.

        Args:
            tap: see tap.py
            name: see tap.py
            query_type: see tap.py
            primary_keys: see tap.py
            replication_key: see tap.py
            schema: the json schema for the stream.
            query: see tap.py
            by_receipt_time: see tap.py
            auto_parsing_mode: see tap.py
            quantization: see tap.py
            rollup: see tap.py
            timeshift: see tap.py

        """
        super().__init__(tap=tap, schema=schema)

        if primary_keys is None:
            primary_keys = []

        self.name = name
        self.query_type = query_type
        self.primary_keys = primary_keys
        self.replication_key = replication_key
        self.query = query
        self.by_receipt_time = by_receipt_time
        self.auto_parsing_mode = auto_parsing_mode
        self.quantization = quantization
        self.rollup = rollup
        self.timeshift = timeshift

    def _wait_for_search_job(self, search_job: dict, delay: int) -> Optional[dict]:
        """Poll until the search job finishes or is cancelled."""
        status = self.conn.search_job_status(search_job)
        while status["state"] != "DONE GATHERING RESULTS":
            if status["state"] == "CANCELLED":
                return None
            time.sleep(delay)
            self.logger.info("")
            status = self.conn.search_job_status(search_job)
            # remove key histogramBuckets from status
            del status["histogramBuckets"]
            self.logger.info(f"Query Status: {status}")
        return status

    def _fetch_search_job_records(
        self,
        search_job: dict,
        record_count: int,
        custom_columns: Dict[str, Any],
        limit: int,
        pagination_delay: int,
    ) -> List[Dict[str, Any]]:
        """Fetch search job results with optional pagination delays."""
        records: List[Dict[str, Any]] = []
        count = 0
        while count < record_count:
            self.logger.info(
                f"Get {self.query_type} {count} of {record_count}, limit={limit}"
            )
            response = self.conn.search_job_records(
                search_job, self.query_type, limit=limit, offset=count
            )
            self.logger.info(f"Got {self.query_type} {count} of {record_count}")

            recs = response[self.query_type]
            for rec in recs:
                records.append({**rec["map"], **custom_columns})

            if len(recs) > 0:
                count = count + len(recs)
                if count < record_count:
                    self.logger.info(
                        "Waiting 2 seconds before next paginated API call to avoid "
                        "rate limit..."
                    )
                    time.sleep(pagination_delay)
            else:
                break  # make sure we exit if nothing comes back
        return records

    def get_records(
        self, context: Optional[Mapping[str, Any]]
    ) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        self.logger.info("Running query in sumologic to get records")

        records: List[Dict[str, Any]] = []
        limit = 10000

        now_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        custom_columns = {
            "start_date": self.config["start_date"],
            "end_date": self.config["end_date"],
            "time_zone": self.config["time_zone"],
            "_SDC_EXTRACTED_AT": now_datetime,
            "_SDC_BATCHED_AT": now_datetime,
            "_SDC_DELETED_AT": None,
        }

        if self.query_type in ["messages", "records"]:
            delay = 5
            pagination_delay = 2
            search_job = self.conn.search_job(
                self.query,
                self.config["start_date"],
                self.config["end_date"],
                self.config["time_zone"],
                self.by_receipt_time,
                self.auto_parsing_mode,
            )
            # self.logger.info(search_job)

            status = self._wait_for_search_job(search_job, delay)
            if status is None:
                self.logger.info("Search job was cancelled, no records to yield")
            else:
                self.logger.info(status["state"])

                record_count = status[f"{self.query_type[:-1]}Count"]
                records = self._fetch_search_job_records(
                    search_job=search_job,
                    record_count=record_count,
                    custom_columns=custom_columns,
                    limit=limit,
                    pagination_delay=pagination_delay,
                )

        elif self.query_type == "metrics":
            response = self.conn.metrics_query(
                self.query,
                self.config["start_date"],
                self.config["end_date"],
                self.quantization,
                self.rollup,
                self.timeshift,
            )
            records = response["queryResult"][0]["timeSeriesList"]["timeSeries"]
            # Enable below lines to add delay iff we've back to back metric queries
            # to be triggered to avoid rate limit.
            # self.logger.info(
            #     "Waiting 15 seconds after metrics query to avoid rate limit..."
            # )
            # time.sleep(15)

        for row in records:
            yield row
