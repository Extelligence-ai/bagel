"""Send emails to specified recipients."""

import logging
import smtplib
from email.message import EmailMessage
from email.utils import formatdate

from src.di import module
from src.pipeline import base


class SendEmailTask(base.Task):
    """Send emails to specified recipients."""

    def __init__(  # noqa: PLR0913
        self,
        sender: str,
        recipients: list[str],
        subject: str,
        body: str,
        smtp_server: str,
        smtp_port: int,
        password: str,
    ) -> None:
        """Initialize the task.

        Args:
            sender (str): The sender email address.
            recipients (list[str]): A list of recipient email addresses.
            subject (str): Subject of the email.
            body (str): Body of the email.
            smtp_server (str): SMTP server address, e.g., "smtp.gmail.com".
            smtp_port (int): SMTP server port, e.g., 587 for TLS.
            password (str): Password for the sender email account.

        """
        self._sender = sender
        self._recipients = recipients
        self._subject = subject
        self._body = body
        self._smtp_server = smtp_server
        self._smtp_port = smtp_port
        self._password = password

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Nothing to setup in this task."""

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        """Execute the task at the given time."""
        msg = EmailMessage()
        msg["From"] = self._sender
        msg["To"] = ", ".join(self._recipients)
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = f"{self._subject} (asof_seconds={asof_seconds:.8f})"
        msg.set_content(self._body)
        with smtplib.SMTP(self._smtp_server, self._smtp_port) as server:
            server.starttls()
            server.login(self._sender, self._password)
            server.send_message(msg)
            logging.info("Sent email to %s", self._recipients)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SendEmailTask
