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
        if isinstance(tables_config, str):
            try:
                parsed_config = json.loads(tables_config)
                return parsed_config
            except json.JSONDecodeError:
                raise ValueError("tables config must be a valid JSON array")
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
            return {}
        if isinstance(params, dict):
            return params
        if isinstance(params, str):
            try:
                parsed = json.loads(params)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
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
            with open(schema_config, "r") as f:
                return json.load(f)
        elif isinstance(schema_config, dict):
            builder = SchemaBuilder()
            builder.add_schema(schema_config)
            return builder.to_schema()
        else:
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

        top_level_params = self._parse_json_params(
            self.config.get("query_params", {}), "top-level query_params"
        )
        if top_level_params:
            merged_query_params.update(top_level_params)

        table_params = self._parse_json_params(
            stream.get("query_params", {}), "table-level query_params"
        )
        if table_params:
            merged_query_params.update(table_params)

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
            return query

        resolved_query = query
        for param_name, param_value in query_params.items():
            placeholder = "{" + param_name + "}"
            if placeholder in resolved_query:
                resolved_query = resolved_query.replace(placeholder, str(param_value))

        return resolved_query

    def discover_streams(self) -> List[SearchJobStream]:  # noqa: C901
        """Return a list of discovered streams."""
        streams = []
        tables_config = self._parse_tables_config(self.config["tables"])

        for idx, stream in enumerate(tables_config):
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

        return streams

    def _get_metrics_schema(self, table_config: Dict) -> Dict:
        """Get predefined schema for metrics queries.

        Args:
            table_config: Table configuration dictionary.

        Returns:
            Schema dictionary for metrics.

        """
        user_primary_keys = table_config.get("primary_keys", [])
        return {
            "type": "object",
            "properties": {
                "metricDefinition": {"type": ["object", "null"]},
                "points": {"type": ["object", "null"]},
            },
            "key_properties": user_primary_keys if user_primary_keys else [],
        }

    def _build_schema_from_fields(self, fields: List, query_type: str) -> Dict:
        """Build schema from Sumo Logic fields.

        Args:
            fields: List of field definitions from Sumo Logic.
            query_type: Type of query (records or messages).

        Returns:
            Schema dictionary.

        """
        schema: Dict[str, Any] = {}
        key_properties: List[str] = []
        base_type = {"type": ["null", "string"]}

        for field in fields:
            field_name = field["name"]
            field_type = field["fieldType"]
            key_field = field["keyField"]

            schema[field_name] = copy.deepcopy(base_type)

            if field_type in ("int", "long"):
                schema[field_name]["type"].append("integer")
            elif field_type == "double":
                schema[field_name]["type"].append("number")

            if key_field:
                key_properties.append(field_name)

        # Add start and end date
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

    def get_schema_for_table(self, table_config: Dict) -> Dict:
        """Detect json schema using a record set of query.

        Args:
            table_config: tables specs

        Returns:
            detected schema

        """
        q: str = table_config.get("query", "")
        query_type = table_config.get("query_type", "messages")

        # Resolve query parameters before making API call
        merged_params = self._merge_query_params(table_config)
        q = self._resolve_query(q, merged_params)

        # For metrics queries, return predefined schema
        if query_type == "metrics":
            return self._get_metrics_schema(table_config)

        if query_type in ("records", "messages"):
            q += " | limit 1"

        sumo = SumoLogic(
            self.config["access_id"], self.config["access_key"], self.config["root_url"]
        )

        fields = sumo.get_sumologic_fields(
            q,
            self.config["start_date"],
            self.config["end_date"],
            self.config["time_zone"],
            table_config.get("by_receipt_time", False),
            table_config.get("auto_parsing_mode", "intelligent"),
            query_type,
            table_config.get("quantization"),
            table_config.get("rollup"),
            table_config.get("timeshift"),
        )

        if query_type in ("records", "messages"):
            return self._build_schema_from_fields(fields, query_type)

        return {}
