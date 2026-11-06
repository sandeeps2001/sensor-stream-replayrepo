# Technical Write-up: Store-Data Ingestion Pipeline (Loyalytics)

*Prepared by Sandeep Saravanan for FramePoint (FP04). The production code is proprietary to Loyalytics, so this describes the design and my contribution without sharing code or client data.*

## 1. Problem
Loyalytics' product, Swan AI, needed customer and store data from clients running Magento e-commerce stores, but there was no reliable way to get that data out of Magento and into the platform. I had one month to deliver.

## 2. My contribution
I designed and built the whole path: the Magento extension (in PHP, which I learned for this project), the Kafka-based ETL pipeline, the validation checks, and the single-store pilot and monitoring. [CONFIRM: list anything a teammate owned, e.g. Swan AI's ingestion endpoint or infrastructure.]

## 3. Architecture
```
Magento store ──► extension (PHP) ──► Kafka topics ──► ETL consumers ──► Swan AI
                  extracts records       [CONFIRM:       validate,
                  + metadata             topics/keys]    transform, load
```
- **Extraction:** [CONFIRM: event hooks on changes, scheduled batch export, or both]
- **Transport:** Kafka. [CONFIRM: how messages were keyed, e.g. by store ID, and partition count]
- **Load:** consumers transformed records into Swan AI's schema. Scale: millions of records across client stores.

## 4. Timestamps and metadata
[CONFIRM each item, and delete anything that wasn't true]
- Source timestamp from Magento (e.g. the record's created/updated time)
- Ingestion timestamp added by the pipeline
- Source identifiers: store ID, record ID, record type
- Handling of late or out-of-order updates: [e.g. "latest updated_at wins" or upserts keyed on record ID]

## 5. Data loss and quality
- Validation checks rejected malformed or incomplete records before they reached Swan AI.
- During the **single-store pilot**, I checked completeness end to end by [CONFIRM: e.g. comparing record counts in Magento against records landed in Swan AI].
- Failures were [CONFIRM: logged, retried, or routed to a dead-letter topic].

## 6. Latency and reliability
- Across our Kafka pipelines (1M+ events/day), I reduced end-to-end latency by 30% by [CONFIRM: what changed, e.g. batching, consumer parallelism, Cosmos DB indexing].
- Measured as [CONFIRM: e.g. time from source event to availability downstream].

## 7. What I'd do differently, and how it connects to FramePoint
- **Clock handling:** we trusted source timestamps from each store's server. For sensors, I'd store device time and ingestion time separately and estimate clock offset and drift per device. I prototyped this in my public demo repo.
- **Loss accounting:** sequence numbers per source make loss measurable directly, instead of reconciling counts afterward.
- **Replay:** recording raw events with versioned sessions makes any bug or model change reproducible.

**Public code:** github.com/sandeeps2001/sensor-stream-replay [CONFIRM once pushed]
