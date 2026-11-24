"""
Pydantic data models for request validation and response serialization.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, field_validator


class WebhookPayload(BaseModel):
    event_id: str = Field(..., min_length=1, description="Unique identifier for the incoming event")
    event_type: str = Field(..., min_length=1, description="Classification of the webhook event")
    source_service: str = Field(..., min_length=1, description="Originating service or platform")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    payload_data: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary event payload data")
    priority: int = Field(default=3, ge=1, le=5, description="Execution priority from 1 (highest) to 5 (lowest)")

    @field_validator("event_id")
    @classmethod
    def validate_event_id_not_empty(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("event_id must not be blank.")
        return trimmed

    @field_validator("event_type")
    @classmethod
    def validate_event_type_not_empty(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("event_type must not be blank.")
        return trimmed


class TaskResult(BaseModel):
    task_id: str
    event_id: str
    status: str = Field(default="queued")
    retry_count: int = 0
    processed_at: Optional[str] = None
    duration_ms: float = 0.0
    error_message: Optional[str] = None


class HealthStatus(BaseModel):
    status: str = "healthy"
    uptime_seconds: float = 0.0
    queued_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
