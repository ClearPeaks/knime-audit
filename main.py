import json
import logging
import os.path
import queue
import shutil
import socket
import zipfile
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import List

from proton.reactor import Container

from audit_info import AuditInfo
from audit_sender import AuditSender
from knime_rest_api import KnimeRestApi
from log_reader_thread import LogReaderThread


def load_config() -> dict:
    """
    Load configuration file from command line arguments
    :return: dict containing configuration settings
    """
    import sys

    # Checks configuration file is provided
    if len(sys.argv) != 2:
        print('ERROR: Provide configuration file. Usage:')
        print('    ./main.py [JSON configuration file]')
        sys.exit()

    return json.loads(open(sys.argv[1]).read())


def configure_logger(config: dict) -> logging.Logger:
    """
    Configure logger with timed rotating files
    :param config: configuration settings
    :return: logger
    """
    logger = logging.getLogger("knime_audit")
    logger.setLevel(getattr(logging, config['log_level'].upper()))
    handler = TimedRotatingFileHandler(config['log_file'], when=config['log_rotation_when'],
                                       interval=config['log_rotation_interval'],
                                       backupCount=config['log_rotation_keep'])
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def generate_job_backup(
        job_id: str,
        job_info: dict,
        workflow_summary: dict,
        workflow_backup_parent_path: str,
        knime_rest_api: KnimeRestApi,
        logger: logging.Logger) -> str:
    """
    Generate backup for storing job information and workflow summary.
    This function creates a daily folder for storing backups, and each job is stored with the creation
    timestamp as a sub-folder. The job folder will contain a job-summary.json, workflow-summary.json and a job.knwf.
    Backup folder layout:
        backup_path/
            yyyymmdd/
                job_id-yyyymmddHHMMSS/
                    workflow-summary.json
                    job-summary.json
                    job_id.knwf
    :param job_id: job_id
    :param job_info: information about the job extracted with the API get_job_info
    :param workflow_summary: summary about the job extracted with the API get_workflow_summary
    :param workflow_backup_parent_path: parent folder where backups are stored
    :param knime_rest_api: class to interact with the Knime REST API
    :param logger: logger
    :return: path where .knwf is being stored
    """
    # Create backup daily folder
    workflow_timestamp = datetime.strptime(job_info["createdAt"].split(".")[0], "%Y-%m-%dT%H:%M:%S")
    workflow_backup_daily_path = os.path.join(
        workflow_backup_parent_path,
        workflow_timestamp.strftime("%Y%m%d")
    )
    if not os.path.exists(workflow_backup_daily_path):
        # This will be created only once a day
        os.makedirs(workflow_backup_daily_path)

    # Create backup daily job folder (no need to check if it already exists, as it will be new always)
    workflow_backup_job_path = os.path.join(
        workflow_backup_daily_path,
        f"{job_id}-{workflow_timestamp.strftime('%Y%m%d%H%M%S')}"
    )
    os.makedirs(workflow_backup_job_path)

    logger.info(f"Storing {job_id} information in: {workflow_backup_job_path}")

    # Store job-summary.json in backup job folder
    with open(os.path.join(workflow_backup_job_path, "job-summary.json"), 'w') as f:
        json.dump(job_info, f)

    # Store workflow-summary.json in backup job folder
    with open(os.path.join(workflow_backup_job_path, "workflow-summary.json"), 'w') as f:
        json.dump(workflow_summary, f)

    # Store .knwf in backup job folder
    knwf_file_path = os.path.join(workflow_backup_job_path, f"{job_id}.knwf")
    with open(knwf_file_path, 'wb') as f:
        knwf_bytes = knime_rest_api.download_workflow_data(job_info["workflow"])
        f.write(knwf_bytes)

    return knwf_file_path


def extract_knwf_info(
        job_id: str,
        workflow_name: str,
        input_path: str,
        output_path: str,
        max_paths: int,
        files_to_keep: List[str],
        logger: logging.Logger) -> List[str]:
    """
    Unzip knwf file with all the job workflow information to a temporary output_path.
    Extract "path" entry from settings.xml from each subfolder to know which datasets are accessed by each node.
    Remove additional files in the output folder, and then compress it back as a knwf file overwriting the input_path.
    :param job_id: job_id
    :param workflow_name: workflow name
    :param input_path: knwf file containing job workflow information. The file is in the backup job folder.
    :param output_path: temporary path to extract the knwf file
    :param max_paths: maximum amount of paths to be returned
    :param files_to_keep: files to be kept from the knwf zip file as a workflow backup
    :param logger: logger
    :return: list of paths in settings.xml of each node
    """
    # Extract knwf information to temporary file
    knwf_output_folder = os.path.join(output_path, job_id)
    with zipfile.ZipFile(input_path, 'r') as zip_ref:
        logger.info(f"Extract knwf files into: {knwf_output_folder}")
        zip_ref.extractall(knwf_output_folder)

    # Process unzipped information
    settings_paths = []
    xml_path_value = '<entry key="path" type="xstring" value="'  # Value to look in settings.xml
    for root, dirs, files in os.walk(os.path.join(knwf_output_folder, workflow_name)):
        # Remove unnecessary files
        for f in files:
            if f not in files_to_keep:
                file_to_remove = os.path.join(root, f)
                logger.info(f"Remove file {file_to_remove}")
                os.remove(file_to_remove)

        # Get all paths in all inner settings.xml
        if len(settings_paths) < max_paths:
            settings_path = os.path.join(root, "settings.xml")
            if os.path.isfile(settings_path):
                logger.info(f"Read {settings_path} to extract paths")
                with open(settings_path, "r") as f:
                    # Find path entry in settings.xml
                    paths_in_dir = [
                        line.replace(xml_path_value, "").replace('"/>', "").strip()
                        for line in f.readlines() if xml_path_value in line]
                    logger.info(f"\tPaths found: {paths_in_dir}")
                    settings_paths.extend(paths_in_dir)
        else:
            # Add ... to indicate that there are actually more paths (only add ... once)
            if "..." not in settings_paths:
                logger.info(f"More than {max_paths} paths reached, skip the rest")
                settings_paths.append("...")

    # Compress the temporary folder replacing the original one
    shutil.make_archive(input_path, 'zip', knwf_output_folder)  # this adds .zip extension in the name
    shutil.move(f"{input_path}.zip", input_path)  # remove .zip extension and replace the original one

    # Remove temporary unzip folder with files
    logger.info(f"Remove temp folder {knwf_output_folder}")
    shutil.rmtree(knwf_output_folder, ignore_errors=True)
    return settings_paths


def main() -> None:
    """
    Main execution thread. Instantiates log reader thread and processes jobs received from that thread.
    """
    config = load_config()
    logger = configure_logger(config)

    q = queue.Queue()

    logger.info("Start log reader thread")
    LogReaderThread(q, config, logger).start()

    knime_rest_api = KnimeRestApi(config)

    # Create backup folder if it does not exist
    backup_path = config["workflow_backup_path"]
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)

    # Process each job_id received from the log reader
    for job_id in iter(q.get, None):
        logger.info(f"Processing job: {job_id}")
        # Get job information & workflow summary
        job_info = knime_rest_api.get_job_info(job_id)
        workflow_summary = knime_rest_api.get_workflow_summary(job_id)
        # Generate backup
        knwf_file_path = generate_job_backup(
            job_id,
            job_info,
            workflow_summary,
            backup_path,
            knime_rest_api,
            logger
        )
        # Get paths from knwf
        paths = extract_knwf_info(
            job_id,
            job_info["workflow"].split("/")[-1],
            knwf_file_path,
            config["temporary_extraction_path"],
            config["max_audit_paths"],
            config["files_to_keep"],
            logger
        )
        # Send audit message
        audit_info = AuditInfo(
            job_id=job_id,
            user_id=job_info["owner"],
            host=socket.gethostname(),
            workflow_state=job_info["state"],
            workflow_timestamp=job_info["createdAt"].split("[")[0],
            error_message=job_info["nodeMessages"][-1]["message"] if "nodeMessages" in job_info and len(job_info["nodeMessages"]) > 0 else "",
            paths=paths,
            audit_path=''.join(os.path.split(knwf_file_path)[:-1])  # Path to backup job folder
        )
        logger.info(f"Send audit info for job {job_id}")
        Container(AuditSender(audit_info, config, logger)).run()


if __name__ == '__main__':
    main()
