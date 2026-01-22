"""sumologic tap class."""

import copy
import datetime
import json
from typing import Any, Dict, List

from genson import SchemaBuilder
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_sumologic.streams import SearchJobStream
from tap_sumologic.sumologic_sdk import SumoLogic


class TapSumoLogic(Tap):
    """sumologic tap class."""

    name = "tap-sumologic"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_id",
            th.StringType,
            required=True,
            description="The access id for authenticating against the Sumologic API.",
        ),
        th.Property(
            "access_key",
            th.StringType,
            required=True,
            description="The access key for authenticating against the Sumologic API.",
        ),
        th.Property(
            "root_url",
            th.StringType,
            default="https://api.sumologic.com/api",  # type: ignore
            description="The Sumologic endpoint for your deployment.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default=(
                datetime.datetime.today() - datetime.timedelta(days=1)  # type: ignore
            ).isoformat(),
            description="The earliest record date to sync. Sets the `from` parameter "
            "for all queries. Format: YYYY-MM-DDTHH:mm:ss",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            default=datetime.datetime.today().isoformat(),  # type: ignore
            description="The latest record date to sync. Sets the `to` parameter "
            "for all queries. Format: YYYY-MM-DDTHH:mm:ss",
        ),
        th.Property(
            "time_zone",
            th.StringType,
            default="UTC",  # type: ignore
            description="The time zone for the queries. Sets the `timeZone` "
            "parameter for all queries",
        ),
        th.Property(
            "query_params",
            th.ObjectType(),
            required=False,
            description="A dictionary of query parameters to substitute "
            "in the query string for ALL tables. Parameters in the query should be "
            "specified as {param_name} and will be replaced with the "
            "corresponding value from this dictionary. This is merged with "
            "table-level query_params (table-level takes precedence). "
            "Example: {'cluster_name': 'my-cluster'}",
        ),
        th.Property(
            "tables",
            required=True,
            description="The list of configs for each table/query/stream.",
            wrapped=th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "query",
                        th.StringType,
                        required=True,
                        description="The Search Job query.",
                    ),
                    th.Property(
                        "table_name",
                        th.StringType,
                        required=True,
                        description="The name for the table/stream.",
                    ),
                    th.Property(
                        "query_type",
                        th.StringType,
                        required=False,
                        default="messages",
                        description="One of 'records', 'messages', 'metrics'. "
                        "Default='messages'. "
                        "Records are the result of a query with aggregation"
                        ". Messages are the result of a query without "
                        "aggregation.",
                    ),
                    th.Property(
                        "primary_keys",
                        th.ArrayType(th.StringType),
                        required=False,
                        default=[],
                        description="Additional fields to include in the primary keys."
                        " Defaults to `[]`.",
                    ),
                    th.Property(
                        "by_receipt_time",
                        th.BooleanType,
                        default=False,  # type: ignore
                        description="Define as true to run the search using "
                        "receipt time. Only applicable on 'records' and 'messages' "
                        "queries.",
                    ),
                    th.Property(
                        "auto_parsing_mode",
                        th.StringType,
                        default="intelligent",  # type: ignore
                        description="The value to provide for the autoParsingMode "
                        "parameter. Default='intelligent' to match the "
                        "behavior of the Sumologic Search Job UI. Only applicable on "
                        "'records' and 'messages' queries.",
                    ),
                    th.Property(
                        "quantization",
                        th.IntegerType,
                        description="Segregates time series data by time period. This "
                        "allows you to create aggregated results in buckets of fixed "
                        "intervals (for example, 5-minute intervals). The value is in "
                        "milliseconds. Only applicable on 'metrics' queries.",
                    ),
                    th.Property(
                        "rollup",
                        th.StringType,
                        description="Can be Avg, Sum, Min, Max, Count or None. Only "
                        "applicable on 'metrics' queries.",
                    ),
                    th.Property(
                        "timeshift",
                        th.IntegerType,
                        description="Shifts the time series from your metrics query by "
                        "the specified amount of time. This can help when comparing a "
                        "time series across multiple time periods. Specified as a "
                        "signed duration in milliseconds. Only applicable on 'metrics' "
                        "queries.",
                    ),
                    th.Property(
                        "query_params",
                        th.ObjectType(),
                        required=False,
                        description="A dictionary of query parameters to substitute "
                        "in the query string. Parameters in the query should be "
                        "specified as {param_name} and will be replaced with the "
                        "corresponding value from this dictionary. "
                        "Example: {'cluster_name': 'my-cluster'}",
                    ),
                    th.Property(
                        "schema",
                        th.CustomType(
                            {
                                "anyOf": [
                                    {"type": "string"},
                                    {"type": "null"},
                                    {"type:": "object"},
                                ]
                            }
                        ),
                        required=False,
                        description="A valid Singer schema or a path-like string "
                        "that provides the path to a `.json` file that "
                        "contains a valid Singer schema. If provided, "
                        "the schema will not be inferred "
                        "from the results of an api call.",
                    ),
                )
            ),
        ),
    ).to_dict()

    def _parse_tables_config(self, tables_config):
        """Parse tables config, handling JSON string format.

        Args:
            tables_config: Tables configuration (list or JSON string).

        Returns:
            Parsed tables configuration as a list.

        """
        self.logger.debug(f"Raw tables_config type: {type(tables_config).__name__}")
        if isinstance(tables_config, str):
            self.logger.info(
                "Tables config received as JSON string (likely from env var)"
            )
            try:
                parsed_config = json.loads(tables_config)
                self.logger.info(
                    f"Successfully parsed tables config: {len(parsed_config)} table(s)"
                )
                return parsed_config
            except json.JSONDecodeError:
                self.logger.error(
                    f"Failed to parse tables config as JSON: {tables_config}"
                )
                raise ValueError("tables config must be a valid JSON array")
        self.logger.debug(
            f"Tables config is already a list: {len(tables_config)} table(s)"
        )
        return tables_config

    def _parse_json_params(self, params, param_name: str = "params") -> Dict:
        """Parse parameters that may be dict or JSON string.

        Args:
            params: Parameters as dict or JSON string.
            param_name: Name for logging purposes.

        Returns:
            Parsed parameters as a dictionary.

        """
        if params is None:
            self.logger.debug(f"{param_name}: None received, returning empty dict")
            return {}
        if isinstance(params, dict):
            self.logger.debug(
                f"{param_name}: dict received with keys: {list(params.keys())}"
            )
            return params
        if isinstance(params, str):
            self.logger.debug(f"{param_name}: string received, attempting JSON parse")
            try:
                parsed = json.loads(params)
                if isinstance(parsed, dict):
                    self.logger.debug(
                        f"{param_name}: parsed successfully "
                        f"with keys: {list(parsed.keys())}"
                    )
                    return parsed
            except json.JSONDecodeError:
                self.logger.warning(f"Failed to parse {param_name} as JSON: {params}")
        self.logger.debug(
            f"{param_name}: returning empty dict (unhandled type or parse failure)"
        )
        return {}

    def _get_schema_for_stream(self, stream: Dict) -> Dict:
        """Get schema for a stream from config or by inference.

        Args:
            stream: Stream configuration dictionary.

        Returns:
            Schema dictionary.

        """
        schema_config = stream.get("schema")
        if isinstance(schema_config, str):
            self.logger.info("Found path to a schema, not doing discovery.")
            with open(schema_config, "r") as f:
                return json.load(f)
        elif isinstance(schema_config, dict):
            self.logger.info("Found schema in config, not doing discovery.")
            builder = SchemaBuilder()
            builder.add_schema(schema_config)
            return builder.to_schema()
        else:
            self.logger.info("No schema found. Inferring schema from API call.")
            return self.get_schema_for_table(stream)

    def _merge_query_params(self, stream: Dict) -> Dict[str, Any]:
        """Merge top-level and table-level query params.

        Top-level query_params (from env var TAP_*_QUERY_PARAMS) takes precedence
        over meltano.yml defaults, and table-level query_params takes precedence
        over top-level.

        Args:
            stream: Stream configuration dictionary.

        Returns:
            Merged query parameters dictionary.

        """
        merged_query_params: Dict[str, Any] = {}

        # Log raw config values for debugging
        raw_top_level = self.config.get("query_params")
        raw_table_level = stream.get("query_params")

        self.logger.info("=" * 80)
        self.logger.info("MERGING QUERY PARAMS")
        self.logger.info("=" * 80)
        self.logger.debug(
            f"Raw query_params - top-level: {raw_top_level} "
            f"(type: {type(raw_top_level).__name__}), "
            f"table-level: {raw_table_level} "
            f"(type: "
            f"{type(raw_table_level).__name__ if raw_table_level else 'None'})"
        )

        # Get top-level query_params from self.config
        # This includes the value from environment variable
        # (which overrides meltano.yml)
        top_level_params = self._parse_json_params(
            self.config.get("query_params", {}), "top-level query_params"
        )
        if top_level_params:
            self.logger.info(f"Step 1: Top-level query_params: {top_level_params}")
            merged_query_params.update(top_level_params)
            self.logger.info(f"After step 1, merged: {merged_query_params}")
        else:
            self.logger.info("Step 1: No top-level query_params found")

        # Get table-level query_params (takes precedence)
        table_params = self._parse_json_params(
            stream.get("query_params", {}), "table-level query_params"
        )
        if table_params:
            self.logger.info(
                f"Step 2: Table-level query_params: {table_params} "
                "(will override top-level)"
            )
            merged_query_params.update(table_params)
            self.logger.info(f"After step 2, merged: {merged_query_params}")
        else:
            self.logger.info("Step 2: No table-level query_params found")

        if merged_query_params:
            self.logger.info("=" * 80)
            self.logger.info(f"FINAL MERGED query_params: {merged_query_params}")
            self.logger.info("=" * 80)
        else:
            self.logger.warning(
                "No query_params found (both top-level and table-level are empty)"
            )

        return merged_query_params

    def _resolve_query(self, query: str, query_params: Dict) -> str:
        """Resolve query by substituting parameter placeholders.

        Args:
            query: Query string with {param_name} placeholders.
            query_params: Dictionary of parameter values.

        Returns:
            Query string with parameters substituted.

        """
        if not query_params:
            self.logger.debug("No query_params provided, returning original query")
            return query

        self.logger.info(f"Original query: {query}")
        self.logger.info(f"Query params to substitute: {query_params}")

        resolved_query = query
        for param_name, param_value in query_params.items():
            placeholder = "{" + param_name + "}"
            if placeholder in resolved_query:
                self.logger.debug(f"Substituting {placeholder} -> {param_value}")
                resolved_query = resolved_query.replace(placeholder, str(param_value))
            else:
                self.logger.warning(
                    f"Placeholder {placeholder} not found in query, skipping"
                )

        self.logger.info(f"Resolved query: {resolved_query}")
        return resolved_query

    def discover_streams(self) -> List[SearchJobStream]:  # noqa: C901
        """Return a list of discovered streams."""
        self.logger.info("=" * 60)
        self.logger.info("Starting stream discovery")
        self.logger.info("=" * 60)
        self.logger.info(
            "TAP-SUMOLOGIC VERSION: This version supports query_params substitution"
        )

        # Log important config values for debugging
        self.logger.info(f"Config start_date: {self.config.get('start_date')}")
        self.logger.info(f"Config end_date: {self.config.get('end_date')}")
        self.logger.info(f"Config time_zone: {self.config.get('time_zone')}")
        self.logger.info(f"Config root_url: {self.config.get('root_url')}")
        self.logger.debug(
            f"Config query_params (raw): {self.config.get('query_params')} "
            f"(type: {type(self.config.get('query_params')).__name__})"
        )

        streams = []
        tables_config = self._parse_tables_config(self.config["tables"])

        for idx, stream in enumerate(tables_config):
            self.logger.info("-" * 40)
            self.logger.info(f"Processing stream {idx + 1}/{len(tables_config)}")
            self.logger.info(f"Table name: {stream.get('table_name')}")
            self.logger.info(f"Query type: {stream.get('query_type', 'messages')}")
            self.logger.info(f"Original query: {stream.get('query')}")

            # Check if query has placeholders
            query_template = stream.get("query", "")
            if "{" in query_template and "}" in query_template:
                self.logger.info(
                    "✓ Query contains placeholders - will be resolved at runtime"
                )
            else:
                self.logger.warning(
                    "⚠ Query does NOT contain placeholders - "
                    "query_params will have no effect"
                )

            self.logger.debug(f"Stream config: {stream}")

            schema = self._get_schema_for_stream(stream)

            query_type = stream.get("query_type", "messages")
            if query_type not in ("records", "messages", "metrics"):
                raise ValueError(
                    f"Invalid query_type: {query_type}. "
                    "Must be one of 'records', 'messages', or 'metrics'."
                )

            primary_keys = stream.get("primary_keys") or schema.get(
                "key_properties", []
            )

            merged_query_params = self._merge_query_params(stream)

            streams.append(
                SearchJobStream(
                    tap=self,
                    name=stream.get("table_name", ""),
                    query_type=query_type,
                    primary_keys=primary_keys,
                    replication_key=stream.get(
                        "replication_key", self.config.get("replication_key", "")
                    ),
                    schema=schema,
                    query=stream.get("query", ""),
                    by_receipt_time=stream.get("by_receipt_time", False),
                    auto_parsing_mode=stream.get("auto_parsing_mode", "intelligent"),
                    quantization=stream.get("quantization"),
                    rollup=stream.get("rollup"),
                    timeshift=stream.get("timeshift"),
                    query_params=merged_query_params if merged_query_params else None,
                )
            )
            self.logger.info(f"Stream '{stream.get('table_name')}' added successfully")

        self.logger.info("=" * 60)
        self.logger.info(f"Stream discovery complete. Total streams: {len(streams)}")
        self.logger.info("=" * 60)
        return streams

    def get_schema_for_table(self, table_config: Dict) -> Dict:
        """Detect json schema using a record set of query.

        Args:
            table_config: tables specs

        Returns:
            detected schema

        """
        schema = {}
        q: str = table_config.get("query", "")
        query_type = table_config.get("query_type", "messages")

        # Resolve query parameters before making API call
        merged_params = self._merge_query_params(table_config)
        q = self._resolve_query(q, merged_params)

        if query_type in ("records", "messages"):
            q += " | limit 1"

        # For metrics queries during schema inference, we don't need to fetch data
        # The schema is predefined, so we can skip the API call entirely
        if query_type == "metrics":
            self.logger.info("Using predefined schema for metrics query.")
            return {
                "type": "object",
                "properties": {
                    "metricDefinition": {"type": ["object", "null"]},
                    "points": {"type": ["object", "null"]},
                },
                "key_properties": [],
            }

        start_date = self.config["start_date"]
        end_date = self.config["end_date"]
        time_zone = self.config["time_zone"]
        base_type = {"type": ["null", "string"]}

        self.logger.info("Running query in sumologic to determine table schema.")
        sumo = SumoLogic(
            self.config["access_id"], self.config["access_key"], self.config["root_url"]
        )

        fields = sumo.get_sumologic_fields(
            q,
            start_date,
            end_date,
            time_zone,
            table_config.get("by_receipt_time", False),
            table_config.get("auto_parsing_mode", "intelligent"),
            query_type,
            table_config.get("quantization"),
            table_config.get("rollup"),
            table_config.get("timeshift"),
        )

        if query_type in ("records", "messages"):
            key_properties = []
            for field in fields:
                field_name = field["name"]
                field_type = field["fieldType"]
                key_field = field["keyField"]

                schema[field_name] = copy.deepcopy(base_type)

                if field_type == "int":
                    schema[field_name]["type"].append("integer")
                elif field_type == "long":
                    schema[field_name]["type"].append("integer")
                elif field_type == "double":
                    schema[field_name]["type"].append("number")
                # a potential bug in the SDK will turn all booleans to True unless
                # this is commented out. This can be uncommented when the fix is
                # implemented
                # elif field_type == "boolean":
                #     schema[field_name]["type"].append("boolean")

                if key_field:
                    key_properties.append(field_name)

            # add start and end date
            schema["start_date"] = base_type
            schema["end_date"] = base_type
            schema["time_zone"] = base_type
            key_properties += ["start_date", "end_date", "time_zone"]
            if query_type == "messages":
                key_properties += ["_messagetime", "_messageid"]

            return {
                "type": "object",
                "properties": schema,
                "key_properties": key_properties,
            }

        return {}
