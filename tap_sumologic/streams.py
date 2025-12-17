"""Stream type classes for tap-sumologic."""

import time
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

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

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        self.logger.info("Running query in sumologic to get records")

        records = []
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
            search_job = self.conn.search_job(
                self.query,
                self.config["start_date"],
                self.config["end_date"],
                self.config["time_zone"],
                self.by_receipt_time,
                self.auto_parsing_mode,
            )
            # self.logger.info(search_job)

            status = self.conn.search_job_status(search_job)
            while status["state"] != "DONE GATHERING RESULTS":
                if status["state"] == "CANCELLED":
                    break
                time.sleep(delay)
                self.logger.info("")
                status = self.conn.search_job_status(search_job)
                # remove key histogramBuckets from status
                del status["histogramBuckets"]
                self.logger.info(f"Query Status: {status}")

            self.logger.info(status["state"])

            if status["state"] == "DONE GATHERING RESULTS":
                record_count = status[f"{self.query_type[:-1]}Count"]
                count = 0
                while count < record_count:
                    self.logger.info(
                        f"Get {self.query_type} {count} of {record_count}, "
                        f"limit={limit}"
                    )
                    response = self.conn.search_job_records(
                        search_job, self.query_type, limit=limit, offset=count
                    )
                    self.logger.info(f"Got {self.query_type} {count} of {record_count}")

                    recs = response[self.query_type]
                    # extract the result maps to put them in the list of records
                    for rec in recs:
                        records.append({**rec["map"], **custom_columns})

                    if len(recs) > 0:
                        count = count + len(recs)
                    else:
                        break  # make sure we exit if nothing comes back

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

        for row in records:
            yield row
