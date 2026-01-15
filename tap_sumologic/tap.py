"""sumologic tap class."""

import copy
import datetime
import json
from typing import Dict, List

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

    def discover_streams(self) -> List[SearchJobStream]:  # type: ignore
        """Return a list of discovered streams."""
        streams = []
        for stream in self.config["tables"]:
            schema_config = stream.get("schema")
            if isinstance(schema_config, str):
                self.logger.info("Found path to a schema, not doing discovery.")
                with open(schema_config, "r") as f:
                    schema = json.load(f)

            elif isinstance(schema_config, dict):
                self.logger.info("Found schema in config, not doing discovery.")
                builder = SchemaBuilder()
                builder.add_schema(schema_config)
                schema = builder.to_schema()

            else:
                self.logger.info("No schema found. Inferring schema from API call.")
                schema = self.get_schema_for_table(stream)

            if stream["query_type"] not in ("records", "messages", "metrics"):
                raise ValueError(
                    f"Invalid query_type: {stream['query_type']}. "
                    "Must be one of 'records' or 'messages'."
                )

            streams.append(
                SearchJobStream(
                    tap=self,
                    name=stream["table_name"],
                    query_type=stream["query_type"],
                    primary_keys=stream["primary_keys"] or schema["key_properties"],
                    replication_key=stream.get(
                        "replication_key", self.config.get("replication_key", "")
                    ),
                    schema=schema,
                    query=stream["query"],
                    by_receipt_time=stream["by_receipt_time"],
                    auto_parsing_mode=stream["auto_parsing_mode"],
                    quantization=stream.get("quantization"),
                    rollup=stream.get("rollup"),
                    timeshift=stream.get("timeshift"),
                )
            )

        return streams

    def get_schema_for_table(self, table_config: Dict) -> Dict:
        """Detect json schema using a record set of query.

        Args:
            table_config: tables specs

        Returns:
            detected schema

        """
        schema = {}
        q: str = table_config["query"]
        if table_config["query_type"] in ("records", "messages"):
            q += " | limit 1"
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
            table_config["by_receipt_time"],
            table_config["auto_parsing_mode"],
            table_config["query_type"],
            table_config.get("quantization"),
            table_config.get("rollup"),
            table_config.get("timeshift"),
        )

        if table_config["query_type"] in ("records", "messages"):
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
            if table_config["query_type"] == "messages":
                key_properties += ["_messagetime", "_messageid"]

            return {
                "type": "object",
                "properties": schema,
                "key_properties": key_properties,
            }

        elif table_config["query_type"] == "metrics":
            return {
                "type": "object",
                "properties": {
                    "metricDefinition": {"type": ["object", "null"]},
                    "points": {"type": ["object", "null"]},
                },
                "key_properties": ["metricDefinition", "points"],
            }

        return {}
