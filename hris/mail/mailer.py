import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from pathlib import Path
from email.utils import formataddr

from django.core.mail import EmailMultiAlternatives, get_connection
from django.template.loader import render_to_string
from django.conf import settings as dj_settings

try:
    from dotenv import load_dotenv, find_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None
    find_dotenv = None


AttachmentType = Union[str, Tuple[str, bytes, Optional[str]]]


def _ensure_env_loaded() -> None:
    """Load .env once, if not already loaded.

    Uses python-dotenv to locate the nearest .env up the directory chain.
    """
    if load_dotenv is None or find_dotenv is None:
        return

    # If key markers already exist, assume env is loaded
    if any(k in os.environ for k in (
        "MAIL_HOST", "MAIL_USERNAME", "MAIL_FROM_ADDRESS"
    )):
        return

    env_path = find_dotenv(usecwd=True)
    if env_path:
        load_dotenv(env_path, override=False)


def _get_mail_settings() -> Dict[str, Any]:
    _ensure_env_loaded()

    mailer = os.getenv("MAIL_MAILER", "smtp")

    host = os.getenv("MAIL_HOST") or getattr(dj_settings, "EMAIL_HOST", None) or "localhost"
    port = int(os.getenv("MAIL_PORT") or getattr(dj_settings, "EMAIL_PORT", 25))
    username = os.getenv("MAIL_USERNAME") or getattr(dj_settings, "EMAIL_HOST_USER", None)
    password = os.getenv("MAIL_PASSWORD") or getattr(dj_settings, "EMAIL_HOST_PASSWORD", None)

    enc = (os.getenv("MAIL_ENCRYPTION") or "").lower()
    use_tls = getattr(dj_settings, "EMAIL_USE_TLS", False)
    use_ssl = getattr(dj_settings, "EMAIL_USE_SSL", False)
    if enc == "tls":
        use_tls, use_ssl = True, False
    elif enc == "ssl":
        use_tls, use_ssl = False, True

    from_address = (
        os.getenv("MAIL_FROM_ADDRESS")
        or getattr(dj_settings, "DEFAULT_FROM_EMAIL", None)
        or (username if username else "no-reply@localhost")
    )
    from_name = os.getenv("MAIL_FROM_NAME") or ""

    return {
        "mailer": mailer,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "use_tls": use_tls,
        "use_ssl": use_ssl,
        "from_address": from_address,
        "from_name": from_name,
    }


def _normalize_recipients(value: Union[str, Iterable[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [v for v in value if v]


class Mail:
    """Fluent mail builder mirip Laravel Mail.

    Contoh:
        Mail.to('user@example.com') \
            .subject('Hello') \
            .view('emails/welcome.html', {'name': 'Hendrik'}) \
            .attach(file_path='/path/file.pdf') \
            .send()
    """

    def __init__(self) -> None:
        self._to: List[str] = []
        self._cc: List[str] = []
        self._bcc: List[str] = []
        self._reply_to: List[str] = []
        self._subject: str = ""
        self._text: Optional[str] = None
        self._html: Optional[str] = None
        self._template_name: Optional[str] = None
        self._context: Dict[str, Any] = {}
        self._attachments: List[Tuple[str, bytes, Optional[str]]] = []
        self._headers: Dict[str, str] = {}
        self._from_address: Optional[str] = None
        self._from_name: Optional[str] = None

    # Recipients
    def to(self, recipients: Union[str, Iterable[str]]) -> "Mail":
        self._to.extend(_normalize_recipients(recipients))
        return self

    def cc(self, recipients: Union[str, Iterable[str]]) -> "Mail":
        self._cc.extend(_normalize_recipients(recipients))
        return self

    def bcc(self, recipients: Union[str, Iterable[str]]) -> "Mail":
        self._bcc.extend(_normalize_recipients(recipients))
        return self

    def reply_to(self, recipients: Union[str, Iterable[str]]) -> "Mail":
        self._reply_to.extend(_normalize_recipients(recipients))
        return self

    # Content
    def subject(self, subject: str) -> "Mail":
        self._subject = subject
        return self

    def text(self, body: str) -> "Mail":
        self._text = body
        return self

    def html(self, html: str) -> "Mail":
        self._html = html
        return self

    def view(self, template_name: str, context: Optional[Dict[str, Any]] = None) -> "Mail":
        self._template_name = template_name
        if context:
            self._context = context
        return self

    def headers(self, headers: Dict[str, str]) -> "Mail":
        self._headers.update(headers)
        return self

    def from_address(self, address: str, name: Optional[str] = None) -> "Mail":
        self._from_address = address
        if name is not None:
            self._from_name = name
        return self

    # Attachments
    def attach(
        self,
        file_path: Optional[str] = None,
        filename: Optional[str] = None,
        mime_type: Optional[str] = None,
        content: Optional[bytes] = None,
    ) -> "Mail":
        if file_path:
            p = Path(file_path)
            name = filename or p.name
            data = p.read_bytes()
            self._attachments.append((name, data, mime_type))
        elif content is not None and filename:
            self._attachments.append((filename, content, mime_type))
        else:
            raise ValueError("attach() requires file_path OR (content and filename)")
        return self

    def attach_bytes(self, data: bytes, filename: str, mime_type: Optional[str] = None) -> "Mail":
        self._attachments.append((filename, data, mime_type))
        return self

    def _render_if_needed(self) -> None:
        if self._template_name:
            html = render_to_string(self._template_name, self._context)
            self._html = html

    def send(self) -> bool:
        settings = _get_mail_settings()

        backend = "django.core.mail.backends.smtp.EmailBackend"
        if settings["mailer"] != "smtp":
            backend = getattr(dj_settings, "EMAIL_BACKEND", backend)

        connection = get_connection(
            backend=backend,
            host=settings["host"],
            port=settings["port"],
            username=settings["username"],
            password=settings["password"],
            use_tls=settings["use_tls"],
            use_ssl=settings["use_ssl"],
            fail_silently=False,
        )

        self._render_if_needed()

        from_name = self._from_name if self._from_name is not None else settings["from_name"]
        from_address = self._from_address if self._from_address is not None else settings["from_address"]
        from_email = formataddr((from_name, from_address)) if from_address else None

        msg = EmailMultiAlternatives(
            subject=self._subject,
            body=(self._text or ""),
            from_email=from_email,
            to=self._to,
            cc=self._cc,
            bcc=self._bcc,
            reply_to=self._reply_to or None,
            headers=self._headers or None,
            connection=connection,
        )

        if self._html:
            msg.attach_alternative(self._html, "text/html")

        for name, data, mime in self._attachments:
            msg.attach(name, data, mime)

        sent = msg.send()
        return bool(sent)


def send_mail(
    to: Union[str, Sequence[str]],
    subject: str,
    body: Optional[str] = None,
    template: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    attachments: Optional[Sequence[AttachmentType]] = None,
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
    from_address: Optional[str] = None,
    from_name: Optional[str] = None,
    reply_to: Optional[Sequence[str]] = None,
    headers: Optional[Dict[str, str]] = None,
    html: Optional[str] = None,
) -> bool:
    mail = Mail().to(to).subject(subject)

    if cc:
        mail.cc(cc)
    if bcc:
        mail.bcc(bcc)
    if reply_to:
        mail.reply_to(reply_to)
    if headers:
        mail.headers(headers)
    if from_address:
        mail.from_address(from_address, from_name)

    if template:
        mail.view(template, context or {})
    if html:
        mail.html(html)
    if body:
        mail.text(body)

    if attachments:
        for att in attachments:
            if isinstance(att, str):
                mail.attach(file_path=att)
            elif isinstance(att, tuple):
                fname = att[0]
                content = att[1]
                mime = att[2] if len(att) > 2 else None
                mail.attach(filename=fname, content=content, mime_type=mime)
            else:
                raise ValueError("Unsupported attachment type")

    return mail.send()


__all__ = ["Mail", "send_mail"]