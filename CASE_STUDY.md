# Flash-sale ticketing on Huawei Cloud

**Verified at 200 end-to-end checkout journeys per second.** In a five-minute qualification on Huawei Cloud Container Engine (CCE), the system dispatched 60,000 synthetic journeys at 200 per second. All eventually fulfilled, with zero drops, checkout errors, or retries. The system issued **59,746 tickets during the five-minute window**, and 95% of journeys completed within **1.686 seconds**.

## Handling a flash sale

When many buyers compete for limited seats, the system must prevent double bookings and issue one ticket per seat even if a payment callback arrives more than once. The qualified configuration used Huawei Cloud DCS for Redis to accept provisional holds and Huawei Cloud RDS for PostgreSQL to commit seat ownership and payment state. Kafka carried committed events to fulfillment workers, and a unique ticket record protected issuance during replay.

The result applies to the tested release, deployment configuration, and five-minute synthetic workload. The qualification used simulated payments and synthetic data.

## What I learned building a 200 journeys/s ticketing system

### 1. The original bottleneck was the synchronous reservation path

Every accepted reservation held an API worker and a PostgreSQL connection while the database locked seats, created the hold and order, recorded idempotency, and committed. Short stalls in writing PostgreSQL’s transaction log kept those slots occupied longer. A burst targeting the same seats added contention. The API could reject excess work quickly, but that did nothing to increase how many buyers could complete checkout.

This changed how I read the benchmarks. In one early hot-seat run, 864 of 1,000 attempts received an overload response. The aggregate server latency looked better partly because the server was rejecting requests quickly. From then on, I tracked admission, durable reservations, and ticket issuance separately.

### 2. Some optimizations worked together. Others failed the comparison.

Redis-first intake, bounded database writes, and separate fulfillment and seat-map consumers were part of the configuration that passed end to end. I qualified them as a bundle, so the result gives me confidence in that configuration without assigning a speedup to each change.

One change I rejected was pipelining a pair of reservation reads. In a matched whole-worker comparison, the candidate was **2.1% slower in the first block and 8.9% faster in the second**. I kept the baseline because the gain was inconsistent. Adding retries to an earlier synchronous benchmark also left failures and increased the work sent through the same constrained path.

### 3. Redis, PostgreSQL, and Kafka each had a specific job

| Component | Why I used it |
| --- | --- |
| **Huawei Cloud DCS for Redis** | A Lua operation could check and claim all requested seats atomically, assign an expiry, and append a reservation command to a Redis Stream. This let the API accept a **provisional** hold before the database commit. |
| **Huawei Cloud RDS for PostgreSQL** | Transactions and uniqueness constraints gave me an authoritative record for bookings, payments, refunds, and tickets. Checkout waited for the hold to become durable in PostgreSQL. |
| **Kafka** | An outbox published committed events for asynchronous fulfillment. Separate consumer groups kept seat-map updates off the ticket issuance path. Consumers could safely process a redelivered event because the database deduplicated it and enforced ticket uniqueness. |

### 4. What changed to reach 200 checkout journeys/s

On Huawei Cloud CCE, the qualified `rc.26` configuration used four API processes, four reservation writers, six fulfillment consumers, and a separate seat projection consumer. The writers committed reservations in small batches. The API could serve a short-lived order-status snapshot from Redis, with a PostgreSQL fallback, so repeated status polls did less database work. Database connections were explicitly bounded.

The acceptance test followed each synthetic buyer through ticket issuance. At **200 journeys/s for 300 seconds**, all **60,000** journeys eventually fulfilled with zero drops, checkout errors, or retries. The system issued **59,746 tickets inside the five-minute window**, and journey p95 was **1.686 seconds**. Independent checks also passed for financial reconciliation, hold expiry, queue drain, and restoration.

### 5. What remains before 200,000 tickets in five minutes

The target requires **666.7 tickets issued per second on average inside the window**, about **3.35×** the verified in-window output. With one ticket per checkout, the offered rate also needs to account for the time it takes the last buyers to finish.

The next bottlenecks are already visible. At 200/s, the wait for a reservation to become durable reached **1.024 seconds at p95**, writer occupancy peaked at **84.7%** over a 30-second segment, and the single publisher spent nearly the whole window in its serial batch operation, including database and broker I/O. In an exploratory **220/s** run, the generator reached its in-flight limit and **5,309 scheduled journeys never started**. All dispatched journeys reconciled, but journey p95 reached **4.980 seconds** and the run failed qualification.

I would first qualify the generator independently, then measure reservation queue age, PostgreSQL commit and WAL headroom, publisher throughput, and consumer lag at each step up. I would test a single hot event separately from traffic spread across events because the Redis intake for an event shares a slot. I would keep the same correctness checks at every step, including duplicate and late callbacks, Redis failover, and reconciliation after rollback. The final acceptance criterion is **200,000 tickets issued by the cutoff**, backed by a complete audit.

## Implementation guide for Huawei Cloud teams

The [implementation guide](docs/implementation-guide.md) covers the architecture, checkout transaction boundaries, QA acceptance, and rollback. It keeps the historical 140 journeys/s comparison separate from the verified 200 journeys/s qualification.

## Try the Docker demo

Follow the [local Docker demo](README.md#run-the-synthetic-demo-locally) to seed a sample event, reserve a seat, and simulate payment. A run from a fresh clone confirmed that three simulated payment callback deliveries produced exactly one persisted ticket. The local demo uses PostgreSQL for reservations and demonstrates the checkout flow. The 200 journeys/s result comes from the separate CCE qualification.
