from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


SSHCapability = Literal["exec", "read", "write"]


class TaskSSHGrantInput(BaseModel):
    profile_id: int = Field(gt=0)
    capabilities: list[SSHCapability] = Field(min_length=1, max_length=3)

    @field_validator("capabilities")
    @classmethod
    def unique_capabilities(
        cls,
        value: list[SSHCapability],
    ) -> list[SSHCapability]:
        return list(dict.fromkeys(value))


class TaskSSHGrantReplace(BaseModel):
    grants: list[TaskSSHGrantInput] = Field(default_factory=list, max_length=50)


class TaskSSHGrantResponse(BaseModel):
    id: int
    task_id: int
    profile_id: int
    profile_name: str
    host: str
    port: int
    username: str
    host_key_fingerprint: str
    profile_revision: int
    current_profile_revision: int
    capabilities: list[SSHCapability]
    valid: bool
    invalid_reason: str | None
    created_by: int | None
    created_at: datetime
    updated_at: datetime


class TaskSSHExecuteRequest(BaseModel):
    command: str = Field(min_length=1, max_length=32768)
    timeout_seconds: int = Field(default=60, ge=1, le=300)
    max_output_bytes: int = Field(
        default=1024 * 1024,
        ge=1024,
        le=1024 * 1024,
    )

    @field_validator("command")
    @classmethod
    def nonblank_command(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("command must not be blank")
        return value


class TaskSSHExecuteResponse(BaseModel):
    exit_code: int
    stdout: str
    stderr: str
    truncated: bool
    duration_ms: int
