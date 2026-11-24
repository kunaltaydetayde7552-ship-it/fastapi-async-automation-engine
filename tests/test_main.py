"""
Unit test suite for the FastAPI Async Automation Engine.
Verifies Pydantic models, rate limiter, priority task queue, retries, and API endpoints.
"""

import asyncio
from pathlib import Path
import sys
import unittest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models import HealthStatus, TaskResult, WebhookPayload
from src.rate_limiter import TokenBucketLimiter
from src.queue_worker import AsyncQueueWorker
from src.app import app
from fastapi.testclient import TestClient
from pydantic import ValidationError


class TestPydanticModels(unittest.TestCase):
    def test_webhook_validation_success(self):
        payload = WebhookPayload(
            event_id="EVT-VALID-01",
            event_type="order.created",
            source_service="shopify",
            priority=2,
            payload_data={"order_id": 5510, "total": 120.0},
        )
        self.assertEqual(payload.event_id, "EVT-VALID-01")
        self.assertEqual(payload.event_type, "order.created")
        self.assertEqual(payload.priority, 2)
        self.assertEqual(payload.payload_data["order_id"], 5510)

    def test_webhook_validation_failure(self):
        # Empty event_id
        with self.assertRaises(ValidationError):
            WebhookPayload(
                event_id="   ",
                event_type="test",
                source_service="tester",
            )

        # Invalid priority outside 1-5 range
        with self.assertRaises(ValidationError):
            WebhookPayload(
                event_id="EVT-BAD",
                event_type="test",
                source_service="tester",
                priority=10,
            )


class TestRateLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_token_bucket_rate_limiter_burst(self):
        # Capacity of 3 tokens
        limiter = TokenBucketLimiter(capacity=3, refill_rate=1.0)
        client = "client_burst"

        # First 3 requests should pass immediately
        self.assertTrue(await limiter.check_allow(client, 1.0))
        self.assertTrue(await limiter.check_allow(client, 1.0))
        self.assertTrue(await limiter.check_allow(client, 1.0))

        # 4th request must be denied as bucket is empty
        self.assertFalse(await limiter.check_allow(client, 1.0))

    async def test_token_bucket_refill(self):
        # Refill rate 10 tokens/sec
        limiter = TokenBucketLimiter(capacity=2, refill_rate=10.0)
        client = "client_refill"

        # Drain capacity
        self.assertTrue(await limiter.check_allow(client, 1.0))
        self.assertTrue(await limiter.check_allow(client, 1.0))
        self.assertFalse(await limiter.check_allow(client, 1.0))

        # Wait 0.25s -> should refill at least 2 tokens
        await asyncio.sleep(0.25)
        tokens = await limiter.get_available_tokens(client)
        self.assertGreaterEqual(tokens, 1.0)
        self.assertTrue(await limiter.check_allow(client, 1.0))


class TestQueueWorker(unittest.IsolatedAsyncioTestCase):
    async def test_async_task_queue_priority_order(self):
        worker = AsyncQueueWorker(max_retries=1, initial_backoff_sec=0.01)
        execution_order = []

        # Worker not started yet, so queue accumulates
        p3 = WebhookPayload(event_id="EVT-P3", event_type="event.low", source_service="test", priority=3)
        p1 = WebhookPayload(event_id="EVT-P1", event_type="event.high", source_service="test", priority=1)
        p5 = WebhookPayload(event_id="EVT-P5", event_type="event.lowest", source_service="test", priority=5)

        # Enqueue in mixed order: P3, P5, P1
        await worker.enqueue(p3)
        await worker.enqueue(p5)
        await worker.enqueue(p1)

        # Register handler tracking order
        async def track_order(p: WebhookPayload):
            execution_order.append(p.event_id)
            return {"status": "ok"}

        worker.register_handler("event.low", track_order)
        worker.register_handler("event.high", track_order)
        worker.register_handler("event.lowest", track_order)

        # Start consumer and await drain
        await worker.start()
        await worker.join()
        await worker.stop()

        # Priority 1 must execute first, then Priority 3, then Priority 5
        self.assertEqual(execution_order, ["EVT-P1", "EVT-P3", "EVT-P5"])

    async def test_retry_backoff_on_failure(self):
        worker = AsyncQueueWorker(max_retries=3, initial_backoff_sec=0.02, backoff_multiplier=1.5)
        payload = WebhookPayload(
            event_id="EVT-RETRY",
            event_type="flaky.service",
            source_service="test",
            priority=1,
        )

        # Injects 2 transient failures, succeeds on 3rd attempt
        worker.inject_temporary_failures("EVT-RETRY", count=2)

        await worker.start()
        task = await worker.enqueue(payload)
        await worker.join()
        await worker.stop()

        final = worker.get_task(task.task_id)
        self.assertIsNotNone(final)
        self.assertEqual(final.status, "completed")
        self.assertEqual(final.retry_count, 2)
        self.assertGreater(final.duration_ms, 0.0)


class TestFastAPIEndpoints(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_fastapi_endpoints(self):
        # Health check
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "healthy")
        self.assertIn("uptime_seconds", data)

        # Webhook submission
        webhook_body = {
            "event_id": "EVT-HTTP-001",
            "event_type": "order.completed",
            "source_service": "payment_gateway",
            "priority": 2,
            "payload_data": {"amount": 250.0},
        }
        post_resp = self.client.post("/api/v1/webhook", json=webhook_body)
        self.assertEqual(post_resp.status_code, 202)
        res_data = post_resp.json()
        self.assertEqual(res_data["status"], "accepted")
        self.assertIn("task_id", res_data)

        # Metrics
        metrics_resp = self.client.get("/api/v1/metrics")
        self.assertEqual(metrics_resp.status_code, 200)
        self.assertIn("total_events_received", metrics_resp.json())


if __name__ == "__main__":
    unittest.main()
