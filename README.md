# tap-sumologic

`tap-sumologic` is a Singer tap for sumologic.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

The main advantage of this version of tap-sumologic vs. other versions on github is that it
includes the ability to set the api parameter `autoParsingMode` to `intelligent`. This is a
feature that also precludes the use of the [sumologic-python-sdk](https://github.com/SumoLogic/sumologic-python-sdk)
which doesn't not allow for setting this parameter either.

Other improvements include the ability to set a start and end date for your search job
queries, as well as other general Meltano SDK improvements.

This tap infers stream schemas based on a sample response from the query.

## Installation

If using via Meltano, add the following lines to your `meltano.yml` file and run the following command:

```yaml
plugins:
  extractors:
    - name: tap-sumologic
      namespace: tap_sumologic
      pip_url: git+https://github.com/Widen/tap-sumologic.git
      executable: tap-sumologic
      capabilities:
        - state
        - catalog
        - discover
      settings:
        - name: access_id
          kind: password
        - name: access_key
          kind: password
        - name: start_date
          kind: string
        - name: end_date
          kind: string
        - name: time_zone
          kind: string
        - name: tables
          kind: array

```

```bash
meltano install extractor tap-rest-api-msdk
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-sumologic --about
```

#### Top level config options

- `access_id`: str: required: The access id for authenticating against the Sumologic API.
- `access_key`: str: required: The access key for authenticating against the Sumologic API.
- `root_url`: str: optional: The Sumologic endpoint for your deployment. Defaults to: `https://api.sumologic.com/api`.
- `start_date`: str: optional: The earliest record date to sync. Same start date for all tables. Format: YYYY-MM-DDTHH:
  mm:ss.
- `end_date`: str: optional: The latest record date to sync. Same end date for all tables. Format: YYYY-MM-DDTHH:mm:ss.
- `time_zone`: str: optional: The time zone for the queries. Sets the parameter for all queries.
- `tables`: array: required: This is the list of configurations for each table/query/stream.

#### Stream level config options

- `query`: str: required: the Sumo Logic Search Job query. Any query that works in the Sumo Logic UI should work in the
  api.
- `table_name`: str: required: the name for the table/stream.
- `query_type`: str: optional: The type of query result to return. Defaults to `messages`. Must be one of `messages`
  or `records`.
- `primary_keys`: array: optional: Additional fields to include in the primary keys. Defaults to `[]`.
- `by_receipt_time`: bool: optional: Define as true to run the search using receipt time. Defaults to `false`.
- `auto_parsing_mode`: str: optional: This enables dynamic parsing, when specified as
  intelligent, Sumo automatically runs field extraction on your JSON log messages when
  you run a search. By default, searches run in performance mode.
- `quantization`: int: optional: Segregates time series data by time period. This allows you to create aggregated
  results in buckets of fixed intervals (for example, 5-minute intervals). The value is in milliseconds. Only applicable
  on 'metrics' queries.
- `rollup`: str: optional: Can be Avg, Sum, Min, Max, Count or None. Only applicable on 'metrics' queries.
- `timeshift`: int: optional: Shifts the time series from your metrics query by the specified amount of time. This can
  help when comparing a time series across multiple time periods. Specified as a signed duration in milliseconds. Only
  applicable on 'metrics' queries.
- `query_params`: object: optional: A dictionary of query parameters to substitute in the query string. Parameters in 
  the query should be specified as `{param_name}` and will be replaced with the corresponding value from this dictionary.
  This is useful when running parameterized queries from orchestration tools like Dagster. Example: `{'cluster_name': 'my-cluster'}`.
- `schema`: optional: A valid Singer schema or a path-like string that provides
  the path to a `.json` file that contains a valid Singer schema. If provided,
  the schema will not be inferred from the results of an api call.

#### Example: Parameterized Metrics Query

You can use query parameters to dynamically substitute values in your Sumo Logic queries. This is particularly useful 
when orchestrating data pipelines with tools like Dagster or Airflow:

```yaml
tables:
  - table_name: cluster_cpu_metrics
    query_type: metrics
    query: >
      metric=aggregate:kube_resource_requests:pod:cpu cluster={cluster_name}
      | quantize to 1h using avg | avg by namespace
    query_params:
      cluster_name: my-production-cluster
```

When triggered via a Dagster job, you can override the `query_params` in your meltano configuration to pass different 
cluster names dynamically.

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

See the [Sumologic API documentation](https://help.sumologic.com/APIs/General-API-Information/API-Authentication)
for details on how to get your `access_id`, `access_key`, as well as for your
[root url](https://help.sumologic.com/APIs/General-API-Information/Sumo-Logic-Endpoints-by-Deployment-and-Firewall-Security).

Most other relevant documentation can be found
[here](https://help.sumologic.com/APIs/Search-Job-API/About-the-Search-Job-API).

## Usage

You can easily run `tap-sumologic` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-sumologic --version
tap-sumologic --help
tap-sumologic --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_sumologic/tests` subfolder and
then run:

```bash
poetry run pytest
```

You can also test the `tap-sumologic` CLI interface directly using `poetry run`:

```bash
poetry run tap-sumologic --help
```

### Continuous Integration

Run through the full suite of tests and linters by running

```bash
poetry run tox -e py
```

These must pass in order for PR's to be merged.

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"
TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-sumologic
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-sumologic --version
# OR run a test `elt` pipeline:
meltano elt tap-sumologic target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
