import requests


class KnimeRestApi:
    """
    Class to interact with KNIME Rest API
    """

    def __init__(self, config: dict) -> None:
        """
        Constructor
        :param config: configuration settings
        """
        import socket
        self.host = socket.gethostname()  # Get host from machine
        self.port = config["knime_rest_api_port"]
        self.base_url = f"https://{self.host}:{self.port}/knime/rest/v4"
        self.headers = {"accept": "application/vnd.mason+json"}
        self.auth = (config["knime_rest_api_user"], config["knime_rest_api_password"])  # Basic authentication
        self.verify = config["ca_cert_file"]

    def get_job_info(self, job_id: str) -> dict:
        """
        Get job information from a finished job_id
        :param job_id: job_id
        :return: job information in dict
        """
        url = f"{self.base_url}/jobs/{job_id}"
        response = requests.get(url, headers=self.headers, auth=self.auth, verify=self.verify)
        return response.json()

    def trigger_swap(self, job_id: str) -> None:
        """
        Trigger swap & creation of workflow summary.
        This is done to force the swap to reduce the waiting time and ensure the workflow summary is contained
        :param job_id: job_id
        """
        url = f"{self.base_url}/jobs/{job_id}/swap"
        requests.put(url, headers=self.headers, auth=self.auth, verify=self.verify)

    def copy_job_in_repo(self, job_id: str, path: str) -> None:
        """
        Copy job as workflow in server repository path, so users can't delete the job
        :param job_id: job_id to be copied as workflow
        :param path: server path to store the workflow files
        """
        url = f"{self.base_url}/repository/{path}:data?from-job={job_id}"
        requests.put(url, headers=self.headers, auth=self.auth, verify=self.verify)

    def download_workflow_data(self, path: str) -> bytes:
        """
        Download workflow with summary and execution statistics from given path
        :param path: workflow path
        :return: workflow .knwf file from response as bytes
        """
        url = f"{self.base_url}/repository/{path}:data"
        response = requests.get(url, headers=self.headers, auth=self.auth, verify=self.verify)
        return response.content

    def get_workflow_summary(self, job_id: str) -> dict:
        """
        Get workflow summary from a finished job_id
        :return: workflow summary in dict
        """
        url = f"{self.base_url}/jobs/{job_id}/workflow-summary?format=JSON&includeExecutionInfo=true"
        response = requests.get(url, headers={"accept": "application/json"}, auth=self.auth, verify=self.verify)
        return response.json()

    def delete_workflow_data(self, path: str) -> None:
        """
        Delete workflow data stored in path
        :param path: workflow path
        """
        url = f"{self.base_url}/repository/{path}"
        requests.delete(url, headers=self.headers, auth=self.auth, verify=self.verify)
