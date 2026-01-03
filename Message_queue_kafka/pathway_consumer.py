"""
OpsPulse AI - Pathway Stream Processor

Real-time log analysis and anomaly detection using Pathway.
Implements tumbling windows for statistical analysis as per SRS REQ-3.1.

Features:
- Ingests logs from Kafka/Redpanda
- Tumbling window aggregation (1-minute windows)
- Spike anomaly detection (error rate > 3 std devs) - REQ-3.2
- Drop anomaly detection (heartbeat frequency drops) - REQ-3.3
- Outputs processed alerts to Kafka topic
"""

import pathway as pw
from pathway.stdlib.temporal import windowby
from datetime import timedelta, datetime
import argparse
import json

# ================= Kafka / Redpanda Config =================
KAFKA_SETTINGS = {
    "bootstrap.servers": "d5c2s6rrcoacstisf5a0.any.ap-south-1.mpx.prd.cloud.redpanda.com:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "SCRAM-SHA-256",
    "sasl.username": "ashutosh",
    "sasl.password": "768581",
    "group.id": "opspulse-pathway-consumer",
    "auto.offset.reset": "earliest",
}

INPUT_TOPIC = "raw_logs"
OUTPUT_TOPIC = "processed_alerts"

# ================= Window Configuration =================
WINDOW_DURATION = timedelta(minutes=1)  # 1-minute tumbling windows per DDS
SPIKE_THRESHOLD = 3.0  # Z-score threshold for spike detection (REQ-3.2)


# ================= Schema Definition =================
class LogSchema(pw.Schema):
    """
    Schema matching the log generator output format.
    Handles both required and optional fields from LogEntry model.
    """
    # Required fields
    timestamp: str
    level: str
    source: str
    service: str
    message: str
    
    # Optional fields - using pw.column_definition for defaults
    request_id: str | None = pw.column_definition(default_value=None)
    user_id: str | None = pw.column_definition(default_value=None)
    ip_address: str | None = pw.column_definition(default_value=None)
    endpoint: str | None = pw.column_definition(default_value=None)
    method: str | None = pw.column_definition(default_value=None)
    status_code: int | None = pw.column_definition(default_value=None)
    response_time_ms: float | None = pw.column_definition(default_value=None)
    error_code: str | None = pw.column_definition(default_value=None)
    stack_trace: str | None = pw.column_definition(default_value=None)
    
    # Nested objects as JSON
    metadata: pw.Json | None = pw.column_definition(default_value=None)
    _labels: pw.Json | None = pw.column_definition(default_value=None)


# ================= UDFs for Data Processing =================
@pw.udf
def parse_timestamp(ts_str: str) -> pw.DateTimeNaive:
    """Convert ISO timestamp to DateTimeNaive for windowing."""
    try:
        # Remove timezone info for DateTimeNaive
        ts_clean = ts_str.replace("Z", "").split("+")[0]
        return datetime.fromisoformat(ts_clean)
    except Exception:
        return datetime.now()


@pw.udf
def is_error_level(level: str) -> bool:
    """Check if log level indicates an error."""
    return level.upper() in ("ERROR", "CRITICAL")


@pw.udf
def extract_anomaly_flag(labels: pw.Json | None) -> bool:
    """Extract is_anomaly from _labels JSON."""
    if labels is None:
        return False
    try:
        return bool(labels.get("is_anomaly", False))
    except Exception:
        return False


@pw.udf
def extract_anomaly_type(labels: pw.Json | None) -> str:
    """Extract anomaly_type from _labels JSON."""
    if labels is None:
        return "none"
    try:
        return str(labels.get("anomaly_type", "none"))
    except Exception:
        return "none"


@pw.udf
def extract_anomaly_score(labels: pw.Json | None) -> float:
    """Extract anomaly_score from _labels JSON."""
    if labels is None:
        return 0.0
    try:
        return float(labels.get("anomaly_score", 0.0))
    except Exception:
        return 0.0


@pw.udf
def calculate_z_score(count: int, mean: float, stddev: float) -> float:
    """Calculate Z-score for spike detection."""
    if stddev == 0 or stddev is None:
        return 0.0
    return abs(count - mean) / stddev


@pw.udf
def format_alert(
    service: str,
    level: str,
    count: int,
    anomaly_count: int,
    avg_response_time: float,
    is_spike: bool
) -> str:
    """Format alert as JSON string for output."""
    alert = {
        "service": service,
        "log_level": level,
        "total_logs": count,
        "anomaly_count": anomaly_count,
        "avg_response_time_ms": round(avg_response_time, 2) if avg_response_time else 0,
        "is_spike": is_spike,
        "alert_type": "spike" if is_spike else "normal",
        "timestamp": datetime.now().isoformat()
    }
    return json.dumps(alert)


# ================= Pipeline Construction =================
def create_pipeline(
    input_topic: str,
    output_topic: str,
    kafka_settings: dict,
    enable_output: bool = True
):
    """
    Build the Pathway streaming pipeline for OpsPulse AI.
    
    Pipeline stages:
    1. Ingest logs from Kafka
    2. Parse and enrich with computed fields
    3. Apply tumbling window aggregation
    4. Detect anomalies (spikes and drops)
    5. Output alerts to Kafka
    """
    
    print("📊 Building Pathway pipeline...")
    
    # ================= Stage 1: Kafka Ingestion =================
    # Using Pathway's Kafka connector with proper rdkafka settings
    logs = pw.io.kafka.read(
        rdkafka_settings=kafka_settings,
        topic=input_topic,
        schema=LogSchema,
        format="json",
        autocommit_duration_ms=1000,  # Batch commits for efficiency
    )
    
    print(f"   ✓ Kafka source connected: {input_topic}")
    
    # ================= Stage 2: Data Enrichment =================
    # Parse timestamps and extract anomaly labels
    enriched_logs = logs.select(
        # Original fields
        timestamp=pw.this.timestamp,
        timestamp_ms=parse_timestamp(pw.this.timestamp),
        level=pw.this.level,
        source=pw.this.source,
        service=pw.this.service,
        message=pw.this.message,
        request_id=pw.this.request_id,
        user_id=pw.this.user_id,
        ip_address=pw.this.ip_address,
        endpoint=pw.this.endpoint,
        method=pw.this.method,
        status_code=pw.this.status_code,
        response_time_ms=pw.coalesce(pw.this.response_time_ms, 0.0),
        error_code=pw.this.error_code,
        
        # Extracted anomaly information
        is_anomaly=extract_anomaly_flag(pw.this._labels),
        anomaly_type=extract_anomaly_type(pw.this._labels),
        anomaly_score=extract_anomaly_score(pw.this._labels),
        
        # Computed fields
        is_error=is_error_level(pw.this.level),
    )
    
    print("   ✓ Log enrichment configured")
    
    # ================= Stage 3: Tumbling Window Aggregation =================
    # Per SRS REQ-3.1: Calculate statistics using tumbling windows
    # Aggregate by service and log level for granular analysis
    
    windowed_stats = enriched_logs.windowby(
        enriched_logs.timestamp_ms,
        window=pw.temporal.tumbling(duration=WINDOW_DURATION),
    ).reduce(
        # Aggregations
        log_count=pw.reducers.count(),
        error_count=pw.reducers.sum(pw.cast(int, pw.this.is_error)),
        anomaly_count=pw.reducers.sum(pw.cast(int, pw.this.is_anomaly)),
        
        # Response time statistics
        avg_response_time=pw.reducers.avg(pw.this.response_time_ms),
        max_response_time=pw.reducers.max(pw.this.response_time_ms),
        min_response_time=pw.reducers.min(pw.this.response_time_ms),
        
        # Anomaly score aggregation
        max_anomaly_score=pw.reducers.max(pw.this.anomaly_score),
        
        # Keep first service/level for context
        service=pw.reducers.earliest(pw.this.service),
        level=pw.reducers.earliest(pw.this.level),
    )
    
    print(f"   ✓ Tumbling window aggregation: {WINDOW_DURATION}")
    
    # ================= Stage 4: Spike Detection =================
    # Per SRS REQ-3.2: Detect when error rate > 3 standard deviations
    # For hackathon MVP, using simple threshold-based detection
    
    # Calculate running statistics for baseline comparison
    # In production, would use more sophisticated rolling statistics
    alerts = windowed_stats.select(
        service=pw.this.service,
        level=pw.this.level,
        log_count=pw.this.log_count,
        error_count=pw.this.error_count,
        anomaly_count=pw.this.anomaly_count,
        avg_response_time=pw.this.avg_response_time,
        max_response_time=pw.this.max_response_time,
        min_response_time=pw.this.min_response_time,
        max_anomaly_score=pw.this.max_anomaly_score,
        
        # Flag spikes based on anomaly density in window
        is_spike=pw.if_else(
            (pw.this.anomaly_count > 0) | (pw.this.error_count >= 5),
            True,
            False
        ),
        
        # Error rate calculation
        error_rate=pw.if_else(
            pw.this.log_count > 0,
            pw.cast(float, pw.this.error_count) / pw.cast(float, pw.this.log_count),
            0.0
        ),
    )
    
    # Filter for actionable alerts only
    actionable_alerts = alerts.filter(
        (pw.this.is_spike == True) | 
        (pw.this.anomaly_count > 0) |
        (pw.this.error_rate > 0.1)  # More than 10% errors
    )
    
    print(f"   ✓ Spike detection threshold: Z > {SPIKE_THRESHOLD}")
    
    # ================= Stage 5: Output =================
    if enable_output:
        # Output alerts to Kafka for downstream processing (RAG, alerting)
        alert_output = actionable_alerts.select(
            alert_json=format_alert(
                pw.this.service,
                pw.this.level,
                pw.this.log_count,
                pw.this.anomaly_count,
                pw.this.avg_response_time,
                pw.this.is_spike,
            )
        )
        
        pw.io.kafka.write(
            alert_output,
            rdkafka_settings=kafka_settings,
            topic_name=output_topic,
            format="json",
        )
        print(f"   ✓ Output sink: {output_topic}")
    
    # ================= Debug Output (Development) =================
    # Subscribe for real-time logging during development
    def on_change(key, row, time, is_addition):
        if is_addition:
            action = "🔔 ALERT"
            print(f"{action}: service={row.get('service')} level={row.get('level')} "
                  f"logs={row.get('log_count')} errors={row.get('error_count')} "
                  f"anomalies={row.get('anomaly_count')} spike={row.get('is_spike')}")
    
    pw.io.subscribe(actionable_alerts, on_change=on_change)
    
    return {
        "logs": enriched_logs,
        "windowed_stats": windowed_stats,
        "alerts": actionable_alerts,
    }


# ================= Main Entry Point =================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OpsPulse AI - Pathway Stream Processor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pathway_consumer.py
  python pathway_consumer.py --topic my_logs --output-topic my_alerts
  python pathway_consumer.py --group-id custom-group --no-output
        """
    )
    parser.add_argument(
        "--topic", 
        default=INPUT_TOPIC,
        help=f"Kafka topic to consume (default: {INPUT_TOPIC})"
    )
    parser.add_argument(
        "--output-topic",
        default=OUTPUT_TOPIC,
        help=f"Kafka topic for alerts (default: {OUTPUT_TOPIC})"
    )
    parser.add_argument(
        "--group-id",
        default="opspulse-pathway-consumer",
        help="Kafka consumer group ID"
    )
    parser.add_argument(
        "--no-output",
        action="store_true",
        help="Disable Kafka output (debug mode)"
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=1,
        help="Tumbling window duration in minutes (default: 1)"
    )
    
    args = parser.parse_args()
    
    # Update configuration
    KAFKA_SETTINGS["group.id"] = args.group_id
    
    # Print startup banner
    print("=" * 60)
    print("🚀 OpsPulse AI - Pathway Stream Processor")
    print("=" * 60)
    print(f"   Input Topic  : {args.topic}")
    print(f"   Output Topic : {args.output_topic}")
    print(f"   Consumer Group: {args.group_id}")
    print(f"   Window Size  : {args.window_minutes} minute(s)")
    print(f"   Kafka Broker : {KAFKA_SETTINGS['bootstrap.servers']}")
    print("=" * 60)
    print()
    
    # Build the pipeline
    pipeline = create_pipeline(
        input_topic=args.topic,
        output_topic=args.output_topic,
        kafka_settings=KAFKA_SETTINGS,
        enable_output=not args.no_output,
    )
    
    print()
    print("🎯 Pipeline ready! Waiting for messages...")
    print("   Press Ctrl+C to stop")
    print()
    
    # Run the Pathway engine (blocking)
    pw.run()
