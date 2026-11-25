"""
Asynchronous in-memory priority task queue worker with exponential backoff retries.
"""

import asyncio
from datetime import datetime, timezone
import time
from typing import Any, Callable, Coroutine, Dict, Optional
import uuid

try:
    from .models import TaskResult, WebhookPayload
except (ImportError, ValueError):
    from models import TaskResult, WebhookPayload


class AsyncQueueWorker:
    """
    Priority task queue consumer.
    Lower priority integer indicates higher execution urgency (1 = Urgent, 5 = Low).
    """

    def __init__(
        self,
        max_retries: int = 3,
        initial_backoff_sec: float = 0.05,
        backoff_multiplier: float = 2.0,
    ):
        self.max_retries = max_retries
        self.initial_backoff_sec = initial_backoff_sec
        self.backoff_multiplier = backoff_multiplier
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._tasks: Dict[str, TaskResult] = {}
        self._counter: int = 0
        self._is_running: bool = False
        self._worker_task: Optional[asyncio.Task] = None
        self._failure_injections: Dict[str, int] = {}
        self._handlers: Dict[str, Callable[[WebhookPayload], Coroutine[Any, Any, Dict[str, Any]]]] = {}

    def register_handler(
        self,
        event_type: str,
        handler: Callable[[WebhookPayload], Coroutine[Any, Any, Dict[str, Any]]],
    ) -> None:
        self._handlers[event_type] = handler

    def inject_temporary_failures(self, event_id: str, count: int) -> None:
        """Configures transient failures for an event ID to test retry and recovery."""
        self._failure_injections[event_id] = count

    async def enqueue(self, payload: WebhookPayload) -> TaskResult:
        task_id = f"task_{uuid.uuid4().hex[:12]}"
        self._counter += 1

        result = TaskResult(
            task_id=task_id,
            event_id=payload.event_id,
            status="queued",
            retry_count=0,
        )
        self._tasks[task_id] = result

        # Item tuple: (priority, counter, task_id, payload, retry_count)
        await self._queue.put((payload.priority, self._counter, task_id, payload, 0))
        return result

    def get_task(self, task_id: str) -> Optional[TaskResult]:
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> Dict[str, TaskResult]:
        return dict(self._tasks)

    def get_counts(self) -> Dict[str, int]:
        queued = sum(1 for t in self._tasks.values() if t.status in ("queued", "retrying"))
        completed = sum(1 for t in self._tasks.values() if t.status == "completed")
        failed = sum(1 for t in self._tasks.values() if t.status == "failed")
        return {
            "queued": queued,
            "completed": completed,
            "failed": failed,
            "total": len(self._tasks),
        }

    async def start(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._consumer_loop())

    async def stop(self) -> None:
        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                _worker_cancelled = True
            self._worker_task = None

    async def join(self) -> None:
        """Waits until all currently queued items are processed."""
        await self._queue.join()

    async def _execute_task_payload(self, payload: WebhookPayload) -> Dict[str, Any]:
        # Check injected transient failure
        if self._failure_injections.get(payload.event_id, 0) > 0:
            self._failure_injections[payload.event_id] -= 1
            raise ConnectionError(
                f"Simulated network timeout communicating with upstream handler for event {payload.event_id}"
            )

        if payload.event_type in self._handlers:
            return await self._handlers[payload.event_type](payload)

        # Default standard execution simulation
        await asyncio.sleep(0.01)
        return {
            "processed": True,
            "event_type": payload.event_type,
            "source": payload.source_service,
        }

    async def _consumer_loop(self) -> None:
        while self._is_running:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                break

            priority, counter, task_id, payload, retry_count = item
            result = self._tasks[task_id]
            result.status = "processing"
            start_time = time.monotonic()

            try:
                await self._execute_task_payload(payload)
                duration = (time.monotonic() - start_time) * 1000.0
                result.status = "completed"
                result.duration_ms = round(duration, 2)
                result.processed_at = datetime.now(timezone.utc).isoformat()
                result.error_message = None
                self._queue.task_done()
            except Exception as exc:
                duration = (time.monotonic() - start_time) * 1000.0
                if retry_count < self.max_retries:
                    new_retry = retry_count + 1
                    result.status = "retrying"
                    result.retry_count = new_retry
                    result.error_message = f"Attempt {new_retry} failed: {str(exc)}"
                    backoff = self.initial_backoff_sec * (self.backoff_multiplier ** retry_count)
                    await asyncio.sleep(backoff)
                    self._counter += 1
                    await self._queue.put((priority, self._counter, task_id, payload, new_retry))
                    self._queue.task_done()
                else:
                    result.status = "failed"
                    result.duration_ms = round(duration, 2)
                    result.processed_at = datetime.now(timezone.utc).isoformat()
                    result.error_message = f"Exhausted {self.max_retries} retries. Final error: {str(exc)}"
                    self._queue.task_done()
