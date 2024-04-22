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
        self.urls = config["activemq_urls"]
        self.queue = config["activemq_queue_name"]
        self.ca_cert_file = config["ca_cert_file"]
        self.key_file = config["key_file"]
        self.key_password = config["key_password"]
        self.body = audit_info.as_xml()
        self.logger = logger
        self.sent = False

    def on_start(self, event: Event) -> None:
        """
        Establish QPID Proton connectivity to the ActiveMQ server using SSL.
        Check SSL configuration: https://qpid.apache.org/releases/qpid-proton-0.39.0/proton/python/docs/proton.html#proton.SSLDomain
        :param event: QPID Proton notification of a state change in the protocol engine.
        """
        self.logger.info("Establishing SSL connectivity with AMQ")
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        ssl_domain.set_credentials(self.ca_cert_file, self.key_file, self.key_password)
        ssl_domain.set_trusted_ca_db(self.ca_cert_file)
        conn = event.container.connect(urls=self.urls, ssl_domain=ssl_domain)
        event.container.create_sender(conn, self.queue)
        self.logger.info("Connection established")

    def on_sendable(self, event: Event) -> None:
        """
        Send a message to the broker queue asynchronously.
        :param event: QPID Proton notification of a state change in the protocol engine.
        """
        if not self.sent:
            self.logger.info(f"Send audit: {self.body}")
            msg = Message(self.body)
            event.sender.send(msg)
            self.sent = True

    def on_accepted(self, event: Event) -> None:
        """
        Message received correctly.
        :param event: QPID Proton notification of a state change in the protocol engine.
        """
        self.logger.info("Accepted message")
        event.connection.close()
