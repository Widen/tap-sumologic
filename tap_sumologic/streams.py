"""Stream type classes for tap-sumologic."""

import json
import time
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Union

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
        query_params: Optional[Union[Dict[str, Any], str]] = None,
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
            query_params: dictionary of parameters to substitute in the query.
                Can be a dict or a JSON string.

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
        self.query_params = self._parse_query_params(query_params)

        # Log stream initialization with query details
        self.logger.info("=" * 80)
        self.logger.info(f"INITIALIZING STREAM: {self.name}")
        self.logger.info(f"Query type: {self.query_type}")
        self.logger.info(f"Original query template: {self.query}")
        self.logger.info(f"Query params (raw): {query_params}")
        self.logger.info(f"Query params (parsed): {self.query_params}")
        self.logger.info("=" * 80)

    def _parse_query_params(
        self, query_params: Optional[Union[Dict[str, Any], str]]
    ) -> Dict[str, Any]:
        """Parse query_params, handling both dict and JSON string formats.

        Args:
            query_params: Query parameters as dict or JSON string.

        Returns:
            Parsed query parameters as a dictionary.

        """
        if query_params is None:
            return {}
        if isinstance(query_params, dict):
            return query_params
        if isinstance(query_params, str):
            try:
                parsed = json.loads(query_params)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                self.logger.warning(
                    f"Failed to parse query_params as JSON: {query_params}"
                )
        return {}

    def _get_resolved_query(self) -> str:
        """Resolve query parameters and return the final query string.

        Substitutes {param_name} placeholders in the query with values
        from query_params dictionary.

        Returns:
            The query string with all parameters substituted.

        """
        if self.query is None:
            self.logger.warning("Query is None, returning empty string")
            return ""

        self.logger.info("*" * 80)
        self.logger.info("QUERY RESOLUTION STARTING")
        self.logger.info(f"Original query template: {self.query}")
        self.logger.info(f"Query params to substitute: {self.query_params}")
        self.logger.info("*" * 80)

        resolved_query = self.query
        if self.query_params:
            for param_name, param_value in self.query_params.items():
                placeholder = "{" + param_name + "}"
                if placeholder in resolved_query:
                    self.logger.info(f"✓ Substituting {placeholder} -> '{param_value}'")
                    resolved_query = resolved_query.replace(
                        placeholder, str(param_value)
                    )
                else:
                    self.logger.warning(
                        f"✗ Placeholder {placeholder} NOT FOUND in query!"
                    )

            self.logger.info("*" * 80)
            self.logger.info("QUERY RESOLUTION COMPLETED")
            self.logger.info(f"Final resolved query: {resolved_query}")
            self.logger.info("*" * 80)
        else:
            self.logger.warning("No query_params provided - using original query as-is")
            self.logger.info(f"Query to execute: {resolved_query}")

        return resolved_query

    def get_records(  # noqa: C901
        self, context: Optional[Mapping[str, Any]]
    ) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        self.logger.info("=" * 60)
        self.logger.info(f"Starting get_records for stream: {self.name}")
        self.logger.info(f"Query type: {self.query_type}")
        self.logger.info("=" * 60)

        records = []
        limit = 10000

        # Get the resolved query with parameters substituted
        resolved_query = self._get_resolved_query()
        self.logger.info(f"Final query to execute: {resolved_query}")

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
                resolved_query,
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
                        # Add delay between paginated API calls to avoid rate limit
                        if count < record_count:
                            self.logger.info(
                                "Waiting 5 seconds before next page "
                                "to avoid rate limit..."
                            )
                            time.sleep(delay)
                    else:
                        break  # make sure we exit if nothing comes back

        elif self.query_type == "metrics":
            self.logger.info("#" * 80)
            self.logger.info("EXECUTING METRICS QUERY")
            self.logger.info("#" * 80)
            self.logger.info(f"Stream name: {self.name}")
            self.logger.info(f"Resolved query: {resolved_query}")
            self.logger.info(f"Start date: {self.config['start_date']}")
            self.logger.info(f"End date: {self.config['end_date']}")
            self.logger.info(f"Time zone: {self.config.get('time_zone', 'UTC')}")
            self.logger.info(f"Quantization: {self.quantization}")
            self.logger.info(f"Rollup: {self.rollup}")
            self.logger.info(f"Timeshift: {self.timeshift}")
            self.logger.info("#" * 80)

            try:
                self.logger.info("Sending query to Sumo Logic API...")
                response = self.conn.metrics_query(
                    resolved_query,
                    self.config["start_date"],
                    self.config["end_date"],
                    self.quantization,
                    self.rollup,
                    self.timeshift,
                )
                self.logger.info("✓ Metrics query executed successfully")

                # Check for errors in response
                if "errors" in response and response["errors"].get("errors"):
                    error_msg = response["errors"]
                    self.logger.error(f"✗ Sumo Logic API returned errors: {error_msg}")
                    raise Exception(f"Metrics query error: {error_msg}")

                metrics_data = response["queryResult"][0]["timeSeriesList"][
                    "timeSeries"
                ]
                self.logger.info(
                    f"✓ Retrieved {len(metrics_data)} time series from Sumo Logic"
                )

                # Add custom columns to each metric
                records = [{**metric, **custom_columns} for metric in metrics_data]
                self.logger.info(
                    f"✓ Prepared {len(records)} records with custom columns"
                )

            except KeyError as e:
                self.logger.error(
                    f"✗ Unexpected response structure from Sumo Logic: {e}"
                )
                self.logger.error(f"Response received: {response}")
                raise
            except Exception as e:
                self.logger.error(f"✗ Error executing metrics query: {e}")
                raise

        self.logger.info("#" * 80)
        self.logger.info(f"YIELDING RECORDS TO LOADER")
        self.logger.info(f"Total records prepared: {len(records)}")
        self.logger.info("#" * 80)

        if records:
            self.logger.info(f"Sample record (first):")
            self.logger.info(f"{records[0]}")
        else:
            self.logger.warning("⚠ No records to yield! Query returned empty result.")

        record_count = 0
        for row in records:
            record_count += 1
            yield row

        self.logger.info("#" * 80)
        self.logger.info(f"✓ Successfully yielded {record_count} records to loader")
        self.logger.info("#" * 80)
