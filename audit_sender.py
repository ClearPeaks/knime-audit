import logging

from proton import Event, Message, SSLDomain
from proton.handlers import MessagingHandler

from audit_info import AuditInfo


class AuditSender(MessagingHandler):

    def __init__(self, audit_info: AuditInfo, config: dict, logger: logging.Logger) -> None:
        """
        Constructor.
        :param audit_info: AuditInfo class containing all the information to be sent and the function to convert to XML.
        :param config: configuration settings.
        :param logger: logger.
        """
        super(AuditSender, self).__init__()
        self.url = config["activemq_url"]
        self.queue = config["activemq_queue_name"]
        self.ca_cert_file = config["ca_cert_file"]
        self.logger = logger
        self.body = audit_info.as_xml()

    def on_start(self, event: Event) -> None:
        """
        Establish QPID Proton connectivity to the ActiveMQ server using SSL.
        Check SSL configuration: https://qpid.apache.org/releases/qpid-proton-0.32.0/proton/python/docs/proton.html#proton.SSLDomain
        :param event: QPID Proton notification of a state change in the protocol engine.
        """
        self.logger.info("Establishing SSL connectivity with AMQ")
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        ssl_domain.set_credentials(cert_file=self.ca_cert_file, key_file="", password=None)
        conn = event.container.connect(self.url, ssl_domain=ssl_domain)
        event.container.create_sender(conn, self.queue)
        self.logger.info("Connection established")

    def on_sendable(self, event: Event) -> None:
        """
        Send a message to the broker queue asynchronously.
        :param event: QPID Proton notification of a state change in the protocol engine.
        """
        while event.sender.credit:
            self.logger.info(f"Send: {self.body}")
            msg = Message(self.body)
            event.sender.send(msg)

    def on_accepted(self, event: Event) -> None:
        """
        Message received correctly.
        :param event: QPID Proton notification of a state change in the protocol engine.
        """
        self.logger.info("Accepted message")
        event.connection.close()

    def on_disconnected(self, event: Event) -> None:
        """
        Connection closed event.
        :param event: QPID Proton notification of a state change in the protocol engine.
        """
        self.logger.info("Disconnected")
