from typing import List


class AuditInfo:

    def __init__(
            self,
            job_id: str,
            user_id: str,
            host: str,
            workflow_state: str,
            workflow_timestamp: str,
            error_message: str,
            paths: List[str],
            audit_path: str
    ) -> None:
        self.job_id = job_id
        self.user_id = user_id
        self.host = host
        self.workflow_state = workflow_state
        self.workflow_timestamp = workflow_timestamp
        self.error_message = error_message
        self.paths = paths
        self.audit_path = audit_path

    def as_xml(self) -> str:
        """
        Convert class into specific audit XML string.
        :return: XML as string
        """
        xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<auditEventList xmlns="http://www.example.com/AuditEvent">
    <auditEvent>
        <actor>
            <id>{self.user_id}</id>
            <name>{self.user_id}</name>
        </actor>
        <application>
            <component>KNIME Server</component>
            <hostName>{self.host}</hostName>
            <name>KNIME</name>
        </application>
         <action>
            <actionType>{self.workflow_state}</actionType>
            <additionalInfo name="jobId">{self.job_id}</additionalInfo>
            <additionalInfo name="errorMessage">{self.error_message}</additionalInfo>
            <additionalInfo name="paths">{','.join(self.paths)}</additionalInfo>
            <additionalInfo name="audit_path">{self.audit_path}</additionalInfo>
            <timestamp>{self.workflow_timestamp}</timestamp>
        </action>
    </auditEvent>
</auditEventList>
        """
        return xml
