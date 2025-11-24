import json

import requests
from logger import logging
from requests.exceptions import HTTPError, RequestException

logger = logging.getLogger(__name__)


def pretty_print_http_error(error_str: str):
    json_error = json.loads(error_str)
    error_detail = json_error.get("detail", [])

    if isinstance(error_detail, dict) or isinstance(error_detail, list):
        pretty_details = []
        for detail in error_detail:
            # Ignore 'input'
            pretty_detail = {
                "type": detail.get("type", "N/A"),
                "loc": detail.get("loc", "N/A"),
                "msg": detail.get("msg", "N/A"),
                "url": detail.get("url", "N/A"),
            }
            pretty_details.append(pretty_detail)

        pretty_detail = json.dumps({"detail": pretty_details}, indent=4)
    elif isinstance(error_detail, str):
        pretty_detail = json_error.get("detail")
    else:
        pretty_detail = json_error

    # Return pretty JSON
    return pretty_detail


class MultivizClient:
    """
    A client for interacting with the MultiViz API.

    Parameters
    ----------
    base_url : str
        The base URL for the MultiViz API.
    api_key : str
        The API key for authenticating requests.
    """

    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key

    def _request(
        self, method, endpoint, params=None, payload=None, ignore_http_statuses=None
    ):
        """
        Private method to perform HTTP requests.

        Parameters
        ----------
        method : str
            The HTTP method to use ('GET', 'POST', 'PUT', 'DELETE').
        endpoint : str
            The API endpoint to send the request to.
        params : dict, optional
            Optional query parameters, by default None.
        payload : dict, optional
            Optional data to include in the request, by default None.
        ignore_http_statuses: list[int]
            List of HTTP status code to not be raised as an error

        Returns
        -------
        dict
            JSON response from the API.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        url = f"{self.base_url}{endpoint}"
        headers = {
            "X-Vibium-Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

        if ignore_http_statuses is None:
            ignore_http_statuses = []

        try:
            response = requests.request(
                method, url, headers=headers, params=params, json=payload
            )

            # Check against ignore http statuses
            if response.status_code in ignore_http_statuses:
                logger.warning(
                    f"Ignored HTTPError: {response.status_code} - {pretty_print_http_error(response.text)}"
                )
                return None

            response.raise_for_status()
            return response.json()
        except HTTPError as exc:
            logger.error(
                f"HTTPError: {exc.response.status_code} - {pretty_print_http_error(exc.response.text)}"
            )
            raise
        except RequestException as exc:
            logger.error(f"RequestException: {str(exc)}")
            raise

    def get_all_sources(self):
        """
        Lists all sources with their source IDs.

        Returns
        -------
        dict
            JSON response from the API containing the list of sources.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = "/sources/"
        return self._request("GET", endpoint)

    def create_waveform_source(self, payload, ignore_existing=False):
        """
        Creates a waveform source with the given source_id and meta information.

        Note that meta requires "location", "assetName", "sensorName", "measurementName" to be specified for this source
        class. An optional field "unitOfMeasure" can be specified with one of the values: g, mss.

        Parameters
        ----------
        payload : dict
            Data for the new source to be created.

        Returns
        -------
        dict
            JSON response from the API containing the created source.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = "/sources/"

        ignore_http_statuses = None
        if ignore_existing:
            ignore_http_statuses = [409]

        return self._request(
            "POST", endpoint, payload=payload, ignore_http_statuses=ignore_http_statuses
        )

    def get_source(self, source_id):
        """
        Retrieves the meta information of the source with source_id.

        Parameters
        ----------
        source_id : str
            The ID of the source.

        Returns
        -------
        dict
            JSON response from the API containing the source details.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}"
        return self._request("GET", endpoint)

    def update_source(self, source_id, payload):
        """
        Updates the meta information of the source with source_id.

        Parameters
        ----------
        source_id : str
            The ID of the source.
        payload : dict
            Data for updating the source.

        Returns
        -------
        dict
            JSON response from the API containing the updated source.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}"
        return self._request("PUT", endpoint, payload=payload)

    def delete_source(self, source_id):
        """
        Deletes the source with source_id.

        Parameters
        ----------
        source_id : str
            The ID of the source.

        Returns
        -------
        dict
            JSON response from the API confirming deletion.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}"
        return self._request("DELETE", endpoint)

    def get_measurements(self, source_id, offset=0, limit=1000):
        """
        Retrieves timestamps and measurement related data for a source with source_id.

        Parameters
        ----------
        source_id : str
            The ID of the source.
        offset : int, optional
            The number of measurements to skip before starting to return them in the response, by default 0.
        limit : int, optional
            The maximum number of measurements to return in the response, by default 1000.
            You can increase this value if needed.

        Returns
        -------
        dict
            JSON response from the API containing measurements.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}/measurements"
        params = {"offset": offset, "limit": limit}
        return self._request("GET", endpoint, params=params)

    def create_waveform_measurement(self, source_id, payload, ignore_existing=False):
        """
        Add a new measurement to source with source_id measured at timestamp time, with duration in seconds and the list
        of captured sample values.

        Parameters
        ----------
        source_id : str
            The ID of the source.
        payload : dict
            Measurement data to be posted.

        Returns
        -------
        dict
            JSON response from the API containing the posted measurement.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}/measurements"

        ignore_http_statuses = None
        if ignore_existing:
            ignore_http_statuses = [409]

        return self._request(
            "POST", endpoint, payload=payload, ignore_http_statuses=ignore_http_statuses
        )

    def get_measurement_by_time(self, source_id, timestamp):
        """
        Retrieves the measurement with timestamp under the source with source_id.

        Parameters
        ----------
        source_id : str
            The ID of the source.
        timestamp : int
            The timestamp of the measurement.

        Returns
        -------
        dict
            JSON response from the API containing the measurement.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}/measurements/{timestamp}"
        return self._request("GET", endpoint)

    def update_measurement_meta(self, source_id, timestamp, payload):
        """
        Update meta information of measurement at timestamp time, of source with source_id.
        It is not possible to update the actual measurement data.

        Parameters
        ----------
        source_id : str
            The ID of the source.
        timestamp : int
            The timestamp of the measurement.
        payload : dict
            Meta information to update.

        Returns
        -------
        dict
            JSON response from the API confirming the update.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}/measurements/{timestamp}"
        return self._request("PUT", endpoint, payload=payload)

    def update_measurement_scalars(self, source_id, timestamp, scalars):
        """
        Updates the scalar values of a measurement for a given source at a specific timestamp.

        Parameters
        ----------
        source_id : str
            The ID of the source.
        timestamp : int
            The timestamp of the measurement.
        scalars : dict
            Scalar values to update.

        Returns
        -------
        dict
            JSON response from the API confirming the update.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}/measurements/{timestamp}/scalars"
        return self._request("PUT", endpoint, payload=scalars)

    def delete_measurement(self, source_id, timestamp):
        """
        Deletes the measurement at timestamp under the source with source_id.

        Parameters
        ----------
        source_id : str
            The ID of the source.
        timestamp : int
            The timestamp of the measurement.

        Returns
        -------
        dict
            JSON response from the API confirming deletion.

        Raises
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        endpoint = f"/sources/{source_id}/measurements/{timestamp}"
        return self._request("DELETE", endpoint)
