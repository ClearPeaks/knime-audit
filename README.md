# knime-audit

This repository contains a Python application to monitor job executions in a KNIME Server using its REST API.
For each job executed is created a backup folder containing job information, workflow summary and a knwf file that 
can be imported in the KNIME Server to recreate the workflow that has been executed.
The main idea is to monitor who is executing every job, even if the user deletes the job or the workflow in the server.
In addition to creating the backup folder, a XML containing the job information is sent to an ActiveMQ queue.

# Overview

The code can be summarized in the following steps:

1. Create a thread that is going to tail the Knime Server tomcat logs.  
2. The thread is going to extract the job_id from the logs and send it to a [thread-safe FIFO queue](https://docs.python.org/es/3/library/queue.html).  
3. The main thread is going to retrieve the job_id and perform the following steps.  
4. Use the `GET https://<serverurl>:<port>/knime/rest/v4/jobs/{job_id}` to retrieve job information.  
5. Use the `GET https://<serverurl>:<port>/knime/rest/v4/jobs/{job_id}/workflow-summary?format=JSON&includeExecutionInfo=true` to retrieve the workflow information.  
6. Use the `GET https://<serverurl>:<port>/knime/rest/v4/repository/{workflow_path}:data` to download the workflow .knwf file.  
7. Unzip the .knwf file to get the settings.xml information and filter the intermediate data we don't want.  
8. Store the job information, the workflow summary, and the filtered .knwf data into a backup folder.  
9. Generate an XML with the job information required and send it to the ActiveMQ queue for auditing.  

# Requirements

The code has been designed for a Python 3.6 version. The requirements include the pip packages:

- `requests` to perform the API calls.  
- `python-qpid-proton` to send the XML to the ActiveMQ queue.  

The QPID Proton client, as it is based on C, requires some additional packages in order to work:

- `python36-devel`
- `openssl-devel` optionally if you need SSL

# Installation in Knime Server

With root privileges do the following:

1. Clone or copy the repository into the KNIME Server.  
2. Ensure the bash script is executable: `chmod u+x knime_audit.sh`  
3. Edit the `knime_audit_config.json` accordingly.  
4. Ensure you have a valid Python3 environment with the requirements mentioned above installed.  
5. Enable the service: 
    ```shell
    cp knime-audit.service /etc/systemd/system/
    systemctl daemon-reload
    systemctl enable knime-audit.service
    ```

