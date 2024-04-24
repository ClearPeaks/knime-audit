import logging
from typing import Callable

import requests


class KnimeRestApi:
    """
    Class to interact with KNIME Rest API
    """

    def __init__(self, config: dict,  logger: logging.Logger) -> None:
        """
        Constructor
        :param config: configuration settings
        """
        self.base_url = config["knime_rest_api_base_url"]
        self.headers = {"accept": "application/vnd.mason+json"}
        self.auth = (config["knime_rest_api_user"], config["knime_rest_api_password"])  # Basic authentication
        self.verify = config["ca_cert_file"]
        self.timeout = config["knime_rest_api_timeout_seconds"]
        self.logger = logger

    def _perform_api_call(self, api_call: Callable, url: str, **kwargs) -> requests.Response:
        """
        Perform API call and log possible errors.
        :param api_call: requests.get / requests.put / requests.delete
        :param url: endpoint to perform the API call
        :param kwargs: arguments for the API call
        :return: response from the API call
        """
        try:
            self.logger.debug(f"Performing API call to {url}")
            response = api_call(url, **kwargs)
            if not response.ok:
                self.logger.error(f"API call response not OK: [{response.status_code}] {response.text}")
                raise
            return response
        except Exception as e:
            self.logger.exception(f"Error exception reaching the API: {e}")
            raise e

    def list_jobs(self) -> dict:
        """
        List all jobs managed by this KNIME Server
        :return: jobs information in dict
        """
        response = self._perform_api_call(
            requests.get,
            url=f"{self.base_url}/jobs/",
            headers=self.headers,
            auth=self.auth,
            verify=self.verify,
            timeout=self.timeout
        )
        return response.json()

    def get_job_info(self, job_id: str) -> dict:
        """
        Get job information from a finished job_id
        :param job_id: job_id
        :return: job information in dict
        """
        response = self._perform_api_call(
            requests.get,
            url=f"{self.base_url}/jobs/{job_id}",
            headers=self.headers,
            auth=self.auth,
            verify=self.verify,
            timeout=self.timeout
        )
        return response.json()

    def get_workflow_summary(self, job_id: str) -> dict:
        """
        Get workflow summary from a finished job_id
        :return: workflow summary in dict
        """
        response = self._perform_api_call(
            requests.get,
            url=f"{self.base_url}/jobs/{job_id}/workflow-summary?format=JSON&includeExecutionInfo=true",
            headers={"accept": "application/json"},
            auth=self.auth,
            verify=self.verify,
            timeout=self.timeout
        )
        return response.json()

    def download_workflow_data(self, path: str) -> bytes:
        """
        Download workflow with summary and execution statistics from given path
        :param path: workflow path
        :return: workflow .knwf file from response as bytes
        """
        response = self._perform_api_call(
            requests.get,
            url=f"{self.base_url}/repository/{path}:data",
            headers=self.headers,
            auth=self.auth,
            verify=self.verify,
            timeout=self.timeout
        )
        return response.content

    def trigger_swap(self, job_id: str) -> None:
        """
        Trigger swap & creation of workflow summary.
        This is done to force the swap to reduce the waiting time and ensure the workflow summary is contained
        :param job_id: job_id
        """
        self._perform_api_call(
            requests.put,
            url=f"{self.base_url}/jobs/{job_id}/swap",
            headers=self.headers,
            auth=self.auth,
            verify=self.verify,
            timeout=self.timeout
        )

    def copy_job_in_repo(self, job_id: str, path: str) -> None:
        """
        Copy job as workflow in server repository path, so users can't delete the job
        :param job_id: job_id to be copied as workflow
        :param path: server path to store the workflow files
        """
        self._perform_api_call(
            requests.put,
            url=f"{self.base_url}/repository/{path}:data?from-job={job_id}",
            headers=self.headers,
            auth=self.auth,
            verify=self.verify,
            timeout=self.timeout
        )

    def delete_workflow_data(self, path: str) -> None:
        """
        Delete workflow data stored in path
        :param path: workflow path
        """
        self._perform_api_call(
            requests.delete,
            url=f"{self.base_url}/repository/{path}",
            headers=self.headers,
            auth=self.auth,
            verify=self.verify,
            timeout=self.timeout
        )
