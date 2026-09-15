"""Taken mostly from Sumologic's python sdk."""

import http.cookiejar as cookielib
import json
import logging
import sys
import time

import requests


class SumoLogic(object):
    """Sumo Logic SDK."""

    def __init__(
        self,
        access_id,
        access_key,
        endpoint=None,
        ca_bundle=None,
        cookie_file="cookies.txt",
    ):
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.DEFAULT_VERSION = "v1"
        self.session.headers = {
            "content-type": "application/json",
            "accept": "application/json",
        }
        if ca_bundle is not None:
            self.session.verify = ca_bundle
        cj = cookielib.FileCookieJar(cookie_file)
        self.session.cookies = cj
        if endpoint is None:
            self.endpoint = self._get_endpoint()
        else:
            self.endpoint = endpoint
        if self.endpoint[-1:] == "/":
            raise Exception("Endpoint should not end with a slash character")
        self.logger = logging.Logger("SumoLogicSDK")

    def _get_endpoint(self):
        """Get a Sumo Logic endpoint.

        Sumo Logic REST API endpoint changes based on the geo location of the client.
        For example, If the client geolocation is Australia then the REST end point is
        https://api.au.sumologic.com/api/v1

        When the default REST endpoint (https://api.sumologic.com/api/v1) is
        used the server responds with a 401 and causes the SumoLogic class
        instantiation to fail and this very unhelpful message is shown 'Full
        authentication is required to access this resource'

        This method makes a request to the default REST endpoint and resolves the
        401 to learn the right endpoint
        """
        self.endpoint = "https://api.sumologic.com/api"
        self.response = self.session.get(
            "https://api.sumologic.com/api/v1/collectors"
        )  # Dummy call to get endpoint
        endpoint = self.response.url.replace(
            "/v1/collectors", ""
        )  # dirty hack to sanitise URI and retain domain
        print("SDK Endpoint", endpoint, file=sys.stderr)
        return endpoint

    def get_versioned_endpoint(self, version):
        """Get the endpoint for the version."""
        return self.endpoint + "/%s" % version

    def get(self, method, params=None, version=None):
        """Set up a GET request."""
        version = version or self.DEFAULT_VERSION
        endpoint = self.get_versioned_endpoint(version)
        r = self.session.get(endpoint + method, params=params)
        if 400 <= r.status_code < 600:
            r.reason = r.text
        r.raise_for_status()
        return r

    def post(self, method, params, headers=None, version=None):
        """Set up a POST request."""
        version = version or self.DEFAULT_VERSION
        endpoint = self.get_versioned_endpoint(version)
        r = self.session.post(
            endpoint + method, data=json.dumps(params), headers=headers
        )
        if 400 <= r.status_code < 600:
            r.reason = r.text
        r.raise_for_status()
        return r

    def search_job(
        self,
        query,
        from_time=None,
        to_time=None,
        time_zone="UTC",
        by_receipt_time=False,
        auto_parsing_mode="intelligent",
    ):
        """Request a Sumo Logic Search Job."""
        params = {
            "query": query,
            "from": from_time,
            "to": to_time,
            "timeZone": time_zone,
            "byReceiptTime": by_receipt_time,
            "autoParsingMode": auto_parsing_mode,
            "requiresRawMessages": True,
        }
        r = self.post("/search/jobs", params)
        return json.loads(r.text)

    def search_job_status(self, search_job):
        """Request the status of a Sumo Logic Search Job."""
        r = self.get("/search/jobs/" + str(search_job["id"]))
        return json.loads(r.text)

    def search_job_records(self, search_job, query_type, limit=None, offset=0):
        """Get the aggregate records of a Sumo Logic Search Job."""
        params = {"limit": limit, "offset": offset}
        r = self.get(f"/search/jobs/{search_job['id']}/{query_type}", params)
        return json.loads(r.text)

    def metrics_query(
        self,
        query,
        from_time,
        to_time,
        quantization,
        rollup,
        timeshift,
    ):
        """Request a Sumo Logic Metrics Query."""
        params = {
            "queries": [
                {
                    "rowId": "A",
                    "query": query,
                    "quantization": quantization,
                    "rollup": rollup,
                    "timeshift": timeshift,
                }
            ],
            "timeRange": {
                "type": "BeginBoundedTimeRange",
                "from": {
                    "type": "Iso8601TimeRangeBoundary",
                    # todo: convert to other timezones according to `time_zone`
                    "iso8601Time": from_time + ".00+00:00",
                },
                "to": {
                    "type": "Iso8601TimeRangeBoundary",
                    # todo: convert to other timezones according to `time_zone`
                    "iso8601Time": to_time + ".00+00:00",
                },
            },
        }
        if quantization is None:
            del params["queries"][0]["quantization"]
        if rollup is None:
            del params["queries"][0]["rollup"]
        if timeshift is None:
            del params["queries"][0]["timeshift"]
        r = self.post("/metricsQueries", params)
        return json.loads(r.text)

    def get_sumologic_fields(
        self,
        q,
        from_time,
        to_time,
        time_zone,
        by_receipt_time,
        auto_parsing_mode,
        query_type,
        quantization=None,
        rollup=None,
        timeshift=None,
    ):
        """Get the fields from a Sumo Logic Search Job or Metrics Query.

        Args:
            q: Sumo Logic query
            from_time: start time
            to_time: end time
            time_zone: timezone
            by_receipt_time: whether to query by receipt time
            auto_parsing_mode: auto parsing mode
            query_type: type of query: records, messages, or metrics
            quantization: quantization
            rollup: rollup
            timeshift: timeshift

        """
        if query_type in ("records", "messages"):
            return self.get_search_job_fields(
                q,
                from_time,
                to_time,
                time_zone,
                by_receipt_time,
                auto_parsing_mode,
                query_type,
            )
        elif query_type == "metrics":
            return self.get_metrics_query_fields(
                q,
                from_time,
                to_time,
                quantization,
                rollup,
                timeshift,
            )

    def get_search_job_fields(
        self,
        q,
        from_time,
        to_time,
        time_zone,
        by_receipt_time,
        auto_parsing_mode,
        query_type,
    ):
        """Get the fields from a Sumo Logic Search Job."""
        fields = []
        delay = 5
        count = 0

        search_job = self.search_job(
            q, from_time, to_time, time_zone, by_receipt_time, auto_parsing_mode
        )

        status = self.search_job_status(search_job)
        while status["state"] != "DONE GATHERING RESULTS":
            if status["state"] == "CANCELLED":
                break
            time.sleep(delay)
            count += 1
            if count == 2:  # don't need to wait for all the results
                break
            status = self.search_job_status(search_job)

        self.logger.info(status["state"])

        if status["state"] in ["DONE GATHERING RESULTS", "GATHERING RESULTS"]:
            response = self.search_job_records(search_job, query_type, limit=1)

            fields = response["fields"]

        return fields

    def get_metrics_query_fields(
        self,
        query,
        from_time,
        to_time,
        quantization,
        rollup,
        timeshift,
    ):
        """Get the fields from a Sumo Logic Metrics Query."""
        metrics_query = self.metrics_query(
            query,
            from_time,
            to_time,
            quantization,
            rollup,
            timeshift,
        )

        if len(metrics_query["errors"]["errors"]) > 0:
            self.logger.error(metrics_query["errors"])
            raise Exception(f"Request error: {metrics_query['errors']}")

        return metrics_query["queryResult"][0]["timeSeriesList"]["timeSeries"]
