"""
FastAPI application and deterministic CLI runner for the Async Automation Engine.
Author: Kunal Tayde (kunaltaydetayde7552-ship-it)
"""

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
import time
from typing import Any, Dict

# Handle execution both as a module and as a standalone script
if __package__ is None or __package__ == "":
    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from models import HealthStatus, TaskResult, WebhookPayload
    from queue_worker import AsyncQueueWorker
    from rate_limiter import TokenBucketLimiter
else:
    from .models import HealthStatus, TaskResult, WebhookPayload
    from .queue_worker import AsyncQueueWorker
    from .rate_limiter import TokenBucketLimiter

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from contextlib import asynccontextmanager

START_TIME = time.time()

# Shared singletons for server runtime
limiter = TokenBucketLimiter(capacity=10, refill_rate=5.0)
worker = AsyncQueueWorker(max_retries=3, initial_backoff_sec=0.05)


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    await worker.start()
    yield
    await worker.stop()


app = FastAPI(
    title="FastAPI Async Automation Engine",
    description="High-performance asynchronous webhook processor with rate-limiting and prioritized task queue.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthStatus)
async def get_health() -> HealthStatus:
    counts = worker.get_counts()
    uptime = time.time() - START_TIME
    return HealthStatus(
        status="healthy",
        uptime_seconds=round(uptime, 2),
        queued_tasks=counts["queued"],
        completed_tasks=counts["completed"],
        failed_tasks=counts["failed"],
    )


@app.post("/api/v1/webhook", status_code=status.HTTP_202_ACCEPTED)
async def ingest_webhook(payload: WebhookPayload, request: Request) -> Dict[str, Any]:
    # Determine client identifier for rate-limiting
    client_ip = request.client.host if request.client else "default_client"

    # Check rate limiter
    allowed = await limiter.check_allow(client_ip, tokens=1.0)
    if not allowed:
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={
                "error": "Rate limit exceeded. Token bucket depleted.",
                "client_id": client_ip,
                "retry_after_seconds": 1.0,
            },
        )

    task_result = await worker.enqueue(payload)
    return {
        "status": "accepted",
        "task_id": task_result.task_id,
        "event_id": payload.event_id,
        "priority": payload.priority,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/tasks/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str) -> TaskResult:
    result = worker.get_task(task_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found",
        )
    return result


@app.get("/api/v1/metrics")
async def get_metrics() -> Dict[str, Any]:
    counts = worker.get_counts()
    all_tasks = worker.get_all_tasks()
    retried_tasks = sum(1 for t in all_tasks.values() if t.retry_count > 0)
    total_retries = sum(t.retry_count for t in all_tasks.values())
    avg_latency = 0.0
    completed = [t.duration_ms for t in all_tasks.values() if t.status == "completed"]
    if completed:
        avg_latency = round(sum(completed) / len(completed), 2)

    return {
        "total_events_received": counts["total"],
        "tasks_completed": counts["completed"],
        "tasks_failed": counts["failed"],
        "tasks_in_flight": counts["queued"],
        "tasks_with_retries": retried_tasks,
        "total_retry_cycles": total_retries,
        "average_duration_ms": avg_latency,
        "limiter_capacity": limiter.capacity,
        "limiter_refill_rate": limiter.refill_rate,
    }


async def run_offline_demo() -> int:
    """Executes deterministic offline demo of rate-limiting, priority queues, and retries."""
    print("=" * 75)
    print("FASTAPI ASYNC AUTOMATION ENGINE")
    print("Deterministic Offline Demonstration Mode (--cli)")
    print("Author: Kunal Tayde (kunaltaydetayde7552-ship-it)")
    print("=" * 75)

    demo_limiter = TokenBucketLimiter(capacity=5, refill_rate=5.0)
    demo_worker = AsyncQueueWorker(max_retries=3, initial_backoff_sec=0.05, backoff_multiplier=1.5)
    await demo_worker.start()

    # Seed 10 sample webhook events
    events = [
        WebhookPayload(
            event_id="EVT-101",
            event_type="order.created",
            source_service="shopify_gateway",
            priority=1,
            payload_data={"order_id": "ORD-9021", "amount": 149.99},
        ),
        WebhookPayload(
            event_id="EVT-102",
            event_type="invoice.paid",
            source_service="stripe_billing",
            priority=2,
            payload_data={"invoice_id": "INV-4412", "amount": 850.00},
        ),
        WebhookPayload(
            event_id="EVT-103",
            event_type="inventory.sync",
            source_service="warehouse_db",
            priority=5,
            payload_data={"sku": "WH-SKU-102", "stock_delta": -12},
        ),
        WebhookPayload(
            event_id="EVT-104",
            event_type="payment.refunded",
            source_service="stripe_billing",
            priority=1,
            payload_data={"refund_id": "REF-0012", "amount": 49.99},
        ),
        WebhookPayload(
            event_id="EVT-105",
            event_type="order.shipped",
            source_service="fedex_tracking",
            priority=3,
            payload_data={"tracking_num": "FX-8849102"},
        ),
        # Events 6-10 arrive in a burst exceeding capacity=5
        WebhookPayload(
            event_id="EVT-106",
            event_type="user.registered",
            source_service="auth_portal",
            priority=4,
            payload_data={"user_id": "USR-8821"},
        ),
        WebhookPayload(
            event_id="EVT-107",
            event_type="subscription.renewed",
            source_service="stripe_billing",
            priority=2,
            payload_data={"sub_id": "SUB-302"},
        ),
        WebhookPayload(
            event_id="EVT-108",
            event_type="cart.abandoned",
            source_service="marketing_engine",
            priority=5,
            payload_data={"cart_id": "CRT-551"},
        ),
        WebhookPayload(
            event_id="EVT-109",
            event_type="lead.captured",
            source_service="hubspot_crm",
            priority=3,
            payload_data={"lead_email": "prospect@example.com"},
        ),
        WebhookPayload(
            event_id="EVT-110",
            event_type="order.created",
            source_service="shopify_gateway",
            priority=1,
            payload_data={"order_id": "ORD-9022", "amount": 299.00},
        ),
    ]

    # Configure transient failure on EVT-104 to demonstrate automatic exponential retry
    demo_worker.inject_temporary_failures("EVT-104", count=2)

    print("\n[Phase 1] Simulating Burst Webhook Ingestion (Capacity: 5 tokens):")
    enqueued_tasks = []
    throttled_events = []

    for ev in events:
        allowed = await demo_limiter.check_allow(client_id="demo_client")
        if allowed:
            res = await demo_worker.enqueue(ev)
            enqueued_tasks.append(res)
            print(f"  [ACCEPTED 202] Event {ev.event_id} (Priority {ev.priority}) -> Task {res.task_id}")
        else:
            throttled_events.append(ev)
            print(f"  [THROTTLED 429] Event {ev.event_id} -> Token bucket exhausted!")

    print(f"\nBurst Summary: {len(enqueued_tasks)} accepted, {len(throttled_events)} throttled.")

    # Simulate token refill after waiting 1.0 second
    print("\n[Phase 2] Waiting for token bucket refill (1.0s @ 5.0 tokens/sec)...")
    await asyncio.sleep(1.05)
    refilled_tokens = await demo_limiter.get_available_tokens("demo_client")
    print(f"  Token bucket refilled: {refilled_tokens:.1f} available tokens.")

    # Re-submit previously throttled events
    print("\n[Phase 3] Resubmitting previously throttled events:")
    for ev in throttled_events:
        allowed = await demo_limiter.check_allow(client_id="demo_client")
        if allowed:
            res = await demo_worker.enqueue(ev)
            enqueued_tasks.append(res)
            print(f"  [ACCEPTED 202] Event {ev.event_id} (Priority {ev.priority}) -> Task {res.task_id}")
        else:
            print(f"  [THROTTLED 429] Event {ev.event_id}")

    # Process all queued tasks through the async consumer pool
    print("\n[Phase 4] Processing tasks through Async Priority Consumer with Retries...")
    await demo_worker.join()
    await demo_worker.stop()

    print("\n[Phase 5] Final Task Execution Telemetry Table:")
    print(f"  {'Task ID':<16} {'Event ID':<10} {'Status':<12} {'Retries':<8} {'Latency (ms)':<14} {'Notes'}")
    print("  " + "-" * 75)

    all_results = demo_worker.get_all_tasks()
    for task_id, t in all_results.items():
        notes = "Clean run"
        if t.retry_count > 0:
            notes = f"Recovered after {t.retry_count} retries"
        print(f"  {t.task_id:<16} {t.event_id:<10} {t.status:<12} {t.retry_count:<8} {t.duration_ms:<14.2f} {notes}")

    counts = demo_worker.get_counts()
    print("\n" + "=" * 75)
    print(f"DEMO COMPLETED: {counts['completed']} completed, {counts['failed']} failed, 100% processed.")
    print("=" * 75)
    return 0


def run_self_test() -> int:
    """Runs internal sanity checks and returns exit code 0."""
    print("Running internal self-tests for fastapi-async-automation-engine...")

    # 1. Pydantic validation
    valid_payload = WebhookPayload(
        event_id="EVT-SELFTEST-1",
        event_type="test.ping",
        source_service="monitor",
        priority=2,
    )
    assert valid_payload.event_id == "EVT-SELFTEST-1"
    assert valid_payload.priority == 2

    # 2. Token bucket burst and drain
    async def _test_limiter() -> None:
        test_limiter = TokenBucketLimiter(capacity=3, refill_rate=1.0)
        assert await test_limiter.check_allow("test", 1.0) is True
        assert await test_limiter.check_allow("test", 1.0) is True
        assert await test_limiter.check_allow("test", 1.0) is True
        assert await test_limiter.check_allow("test", 1.0) is False

    asyncio.run(_test_limiter())

    # 3. Queue worker processing
    async def _test_worker() -> None:
        test_worker = AsyncQueueWorker(max_retries=2, initial_backoff_sec=0.01)
        await test_worker.start()
        task = await test_worker.enqueue(valid_payload)
        assert task.status == "queued"
        await test_worker.join()
        await test_worker.stop()
        final_task = test_worker.get_task(task.task_id)
        assert final_task is not None
        assert final_task.status == "completed"

    asyncio.run(_test_worker())

    print("All self-tests passed successfully (3/3 checks passed).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="FastAPI Async Automation Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--cli", "--demo", action="store_true", help="Run deterministic offline demo")
    parser.add_argument("--self-test", action="store_true", help="Run internal self-tests")
    parser.add_argument("--serve", action="store_true", help="Run ASGI server via Uvicorn")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=8000, help="Server port")

    args = parser.parse_args()

    if args.self_test:
        return run_self_test()

    if args.serve:
        import uvicorn
        print(f"Starting ASGI server on {args.host}:{args.port}...")
        uvicorn.run("src.app:app", host=args.host, port=args.port, reload=False)
        return 0

    if args.cli or len(sys.argv) == 1:
        return asyncio.run(run_offline_demo())

    return 0


if __name__ == "__main__":
    sys.exit(main())
