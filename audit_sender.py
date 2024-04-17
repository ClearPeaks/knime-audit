import logging

from proton import Event, Message, SSLDomain
from proton.handlers import MessagingHandler

from audit_info import AuditInfo


class AuditSender(MessagingHandler):

    def __init__(self, audit_info: AuditInfo, config: dict, logger: logging.Logger) -> None:
        """
        Constructor
        :param config: configuration settings
        """
        super(AuditSender, self).__init__()
        self.url = config["activemq_url"]
        self.queue = config["activemq_queue_name"]
        self.ca_cert_file = config["ca_cert_file"]
        self.logger = logger
        self.body = audit_info.as_xml()

    def on_start(self, event: Event) -> None:
        self.logger.info("connecting ...")
        # https://qpid.apache.org/releases/qpid-proton-0.32.0/proton/python/docs/proton.html#proton.SSLDomain
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)  # Is this mode?
        ssl_domain.set_credentials(cert_file=self.ca_cert_file, key_file="", password=None)
        conn = event.container.connect(self.url, ssl_domain=ssl_domain)
        event.container.create_sender(conn)  # Añadir self.host como parametro tmbn?

    def on_sendable(self, event: Event) -> None:
        # Async send message
        while event.sender.credit:
            self.logger.info(f"Send: {self.body}")
            msg = Message(self.body)
            event.sender.send(msg)

    def on_accepted(self, event: Event) -> None:
        # Message received
        self.logger.info("accepted")
        event.connection.close()

    def on_disconnected(self, event: Event) -> None:
        # Connection closed
        self.logger.info("disconnected")
