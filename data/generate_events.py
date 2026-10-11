"""Generate deterministic, synthetic e-commerce events for the lakehouse demo."""

from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

EVENTS = ("product_view", "add_to_cart", "checkout", "purchase")
REGIONS = ("ap-south-1", "us-east-1", "eu-west-1")
DEVICES = ("mobile", "desktop", "tablet")
CATALOG = (
    ("SKU-100", "cloud", 49.00),
    ("SKU-200", "storage", 89.50),
    ("SKU-300", "networking", 129.00),
    ("SKU-400", "security", 74.25),
    ("SKU-500", "analytics", 199.00),
    ("SKU-600", "compute", 159.00),
)
FIELDS = (
    "event_id",
    "event_timestamp",
    "user_id",
    "session_id",
    "event_type",
    "product_id",
    "category",
    "region",
    "device",
    "quantity",
    "unit_price_usd",
)


def generate(rows: int, seed: int) -> list[dict[str, object]]:
    """Create plausible but fictional clickstream journeys spanning 90 days."""
    rng = random.Random(seed)
    now = datetime.now(UTC).replace(microsecond=0)
    output: list[dict[str, object]] = []

    while len(output) < rows:
        session_id = f"ses_{uuid.UUID(int=rng.getrandbits(128))}"
        user_id = f"usr_{rng.randint(1, max(25, rows // 8)):06d}"
        product_id, category, price = rng.choice(CATALOG)
        started = now - timedelta(days=rng.randrange(90), seconds=rng.randrange(86_400))
        region = rng.choice(REGIONS)
        device = rng.choice(DEVICES)
        journey = ["product_view"]
        if rng.random() < 0.42:
            journey.append("add_to_cart")
            if rng.random() < 0.68:
                journey.append("checkout")
                if rng.random() < 0.72:
                    journey.append("purchase")

        for index, event_type in enumerate(journey):
            if len(output) >= rows:
                break
            event_time = started + timedelta(minutes=index * rng.randint(1, 8))
            output.append(
                {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                    "event_timestamp": event_time.isoformat(),
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_type": event_type,
                    "product_id": product_id,
                    "category": category,
                    "region": region,
                    "device": device,
                    "quantity": 1 if event_type == "purchase" else 0,
                    "unit_price_usd": f"{price:.2f}" if event_type == "purchase" else "0.00",
                }
            )
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000, help="Maximum events to create")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable data")
    parser.add_argument("--output", type=Path, default=Path("data/generated/events.csv"))
    args = parser.parse_args()
    if args.rows < 1 or args.rows > 1_000_000:
        parser.error("--rows must be between 1 and 1,000,000")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(generate(args.rows, args.seed))
    print(f"Wrote {args.rows:,} synthetic events to {args.output}")


if __name__ == "__main__":
    main()
