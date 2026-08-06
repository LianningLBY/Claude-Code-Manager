from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _SSHProfileConnectionFields(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(min_length=1, max_length=255)

    @field_validator("name", "host", "username")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized


class SSHProfileCreate(_SSHProfileConnectionFields):
    key_path: str = Field(min_length=1, max_length=1000)
    host_key_value: str = Field(min_length=1, max_length=16384)
    enabled: bool = True


class SSHProfileUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    host: str | None = Field(default=None, min_length=1, max_length=253)
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = Field(default=None, min_length=1, max_length=255)
    key_path: str | None = Field(default=None, max_length=1000)
    host_key_value: str | None = Field(default=None, min_length=1, max_length=16384)
    enabled: bool | None = None

    @field_validator("name", "host", "username")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized

    @field_validator("key_path")
    @classmethod
    def blank_key_path_keeps_existing(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        return value.strip()


class SSHHostKeyProbeRequest(BaseModel):
    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=22, ge=1, le=65535)
    timeout_seconds: float = Field(default=10, gt=0, le=30)

    @field_validator("host")
    @classmethod
    def strip_host(cls, value: str) -> str:
        return value.strip()


class SSHHostKeyProbeResponse(BaseModel):
    key_type: str
    host_key_value: str
    fingerprint: str


class SSHProfileResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    host: str
    port: int
    username: str
    key_path_hint: str
    public_key_fingerprint: str
    host_key_type: str
    host_key_fingerprint: str
    revision: int
    enabled: bool
    created_by: int | None
    last_tested_at: datetime | None
    last_test_ok: bool | None
    last_error_code: str | None
    last_error_detail: str | None
    created_at: datetime
    updated_at: datetime


class SSHProfileTestResponse(BaseModel):
    ok: bool
    error_code: str | None = None
    detail: str | None = None
