from typing import ClassVar, Set, Optional

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    input_file: str = Field(
        ...,
        title="Input JSONL File",
        description="Path to the input JSONL file that will be partitioned.",
    )


class OutputModel(BaseModel):
    protocols: ClassVar[Set[str]] = {'conn', 'dns', 'ftp', 'sip', 'smtp', 'ssh', 'ssl', 'http'}

    conn: Optional[str] = Field(
        title="conn.log",
        description="Records network connection activity observed by Zeek, capturing metadata about communications between originators and responders across both stateful (e.g., TCP) and stateless (e.g., UDP) protocols."
    )
    dns: Optional[str] = Field(
        title="dns.log",
        description="Captures Domain Name System queries and responses used for hostname resolution."
    )
    ftp: Optional[str] = Field(
        title="ftp.log",
        description="Logs File Transfer Protocol sessions including commands and file transfers."
    )
    sip: Optional[str] = Field(
        title="sip.log",
        description="Captures Session Initiation Protocol signaling used in VoIP communications."
    )
    smtp: Optional[str] = Field(
        title="smtp.log",
        description="Records Simple Mail Transfer Protocol transactions related to email delivery."
    )
    ssh: Optional[str] = Field(
        title="ssh.log",
        description="Captures Secure Shell connection metadata including authentication attempts and encryption parameters."
    )
    ssl: Optional[str] = Field(
        title="ssl.log",
        description="Records TLS/SSL handshake metadata and certificate details from encrypted connections."
    )
    http: Optional[str] = Field(
        title="http.log",
        description="Logs HTTP requests and responses exchanged between clients and web servers."
    )
