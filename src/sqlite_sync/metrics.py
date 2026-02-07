"""
metrics.py - Enterprise Observability & Monitoring

Provides:
- Prometheus-compatible metrics
- Structured JSON logging
- Health check with detailed status
- Performance tracing
"""

import time
import json
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from enum import Enum
from contextlib import contextmanager
import os


# =============================================================================
# Metric Types
# =============================================================================

class MetricType(Enum):
    """Prometheus metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


@dataclass
class MetricValue:
    """Single metric value with labels."""
    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class HistogramBucket:
    """Histogram bucket."""
    le: float  # Less than or equal
    count: int = 0


# =============================================================================
# Metric Collectors
# =============================================================================

class Counter:
    """Prometheus-style counter metric."""
    
    def __init__(self, name: str, help_text: str, labels: List[str] = None):
        self.name = name
        self.help = help_text
        self.labels = labels or []
        self._values: Dict[tuple, float] = {}
        self._lock = threading.Lock()
    
    def inc(self, value: float = 1, **label_values) -> None:
        """Increment counter."""
        key = self._label_key(label_values)
        with self._lock:
            self._values[key] = self._values.get(key, 0) + value
    
    def get(self, **label_values) -> float:
        """Get current value."""
        key = self._label_key(label_values)
        return self._values.get(key, 0)
    
    def collect(self) -> List[MetricValue]:
        """Collect all values for export."""
        with self._lock:
            return [
                MetricValue(
                    name=self.name,
                    value=value,
                    labels=dict(zip(self.labels, key))
                )
                for key, value in self._values.items()
            ]
    
    def _label_key(self, label_values: dict) -> tuple:
        return tuple(label_values.get(l, "") for l in self.labels)


class Gauge:
    """Prometheus-style gauge metric."""
    
    def __init__(self, name: str, help_text: str, labels: List[str] = None):
        self.name = name
        self.help = help_text
        self.labels = labels or []
        self._values: Dict[tuple, float] = {}
        self._lock = threading.Lock()
    
    def set(self, value: float, **label_values) -> None:
        """Set gauge value."""
        key = self._label_key(label_values)
        with self._lock:
            self._values[key] = value
    
    def inc(self, value: float = 1, **label_values) -> None:
        """Increment gauge."""
        key = self._label_key(label_values)
        with self._lock:
            self._values[key] = self._values.get(key, 0) + value
    
    def dec(self, value: float = 1, **label_values) -> None:
        """Decrement gauge."""
        self.inc(-value, **label_values)
    
    def get(self, **label_values) -> float:
        """Get current value."""
        key = self._label_key(label_values)
        return self._values.get(key, 0)
    
    def collect(self) -> List[MetricValue]:
        """Collect all values for export."""
        with self._lock:
            return [
                MetricValue(
                    name=self.name,
                    value=value,
                    labels=dict(zip(self.labels, key))
                )
                for key, value in self._values.items()
            ]
    
    def _label_key(self, label_values: dict) -> tuple:
        return tuple(label_values.get(l, "") for l in self.labels)


class Histogram:
    """Prometheus-style histogram metric."""
    
    DEFAULT_BUCKETS = (
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 
        0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float('inf')
    )
    
    def __init__(
        self, 
        name: str, 
        help_text: str, 
        labels: List[str] = None,
        buckets: tuple = None
    ):
        self.name = name
        self.help = help_text
        self.labels = labels or []
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self._values: Dict[tuple, dict] = {}
        self._lock = threading.Lock()
    
    def observe(self, value: float, **label_values) -> None:
        """Observe a value."""
        key = self._label_key(label_values)
        
        with self._lock:
            if key not in self._values:
                self._values[key] = {
                    "count": 0,
                    "sum": 0.0,
                    "buckets": {b: 0 for b in self.buckets}
                }
            
            data = self._values[key]
            data["count"] += 1
            data["sum"] += value
            
            for bucket in self.buckets:
                if value <= bucket:
                    data["buckets"][bucket] += 1
    
    @contextmanager
    def time(self, **label_values):
        """Context manager to time an operation."""
        start = time.perf_counter()
        try:
            yield
        finally:
            self.observe(time.perf_counter() - start, **label_values)
    
    def collect(self) -> List[MetricValue]:
        """Collect all values for export."""
        results = []
        
        with self._lock:
            for key, data in self._values.items():
                labels = dict(zip(self.labels, key))
                
                # Sum
                results.append(MetricValue(
                    name=f"{self.name}_sum",
                    value=data["sum"],
                    labels=labels
                ))
                
                # Count
                results.append(MetricValue(
                    name=f"{self.name}_count",
                    value=data["count"],
                    labels=labels
                ))
                
                # Buckets
                for le, count in data["buckets"].items():
                    bucket_labels = {**labels, "le": str(le)}
                    results.append(MetricValue(
                        name=f"{self.name}_bucket",
                        value=count,
                        labels=bucket_labels
                    ))
        
        return results
    
    def _label_key(self, label_values: dict) -> tuple:
        return tuple(label_values.get(l, "") for l in self.labels)


# =============================================================================
# Metrics Registry
# =============================================================================

class MetricsRegistry:
    """Global metrics registry."""
    
    def __init__(self, prefix: str = "sqlite_sync"):
        self.prefix = prefix
        self._metrics: Dict[str, Counter | Gauge | Histogram] = {}
        self._lock = threading.Lock()
    
    def counter(
        self, 
        name: str, 
        help_text: str, 
        labels: List[str] = None
    ) -> Counter:
        """Register or get a counter metric."""
        full_name = f"{self.prefix}_{name}"
        
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = Counter(full_name, help_text, labels)
            return self._metrics[full_name]
    
    def gauge(
        self, 
        name: str, 
        help_text: str, 
        labels: List[str] = None
    ) -> Gauge:
        """Register or get a gauge metric."""
        full_name = f"{self.prefix}_{name}"
        
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = Gauge(full_name, help_text, labels)
            return self._metrics[full_name]
    
    def histogram(
        self, 
        name: str, 
        help_text: str, 
        labels: List[str] = None,
        buckets: tuple = None
    ) -> Histogram:
        """Register or get a histogram metric."""
        full_name = f"{self.prefix}_{name}"
        
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = Histogram(full_name, help_text, labels, buckets)
            return self._metrics[full_name]
    
    def collect_all(self) -> List[MetricValue]:
        """Collect all metrics."""
        results = []
        
        with self._lock:
            for metric in self._metrics.values():
                results.extend(metric.collect())
        
        return results
    
    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format."""
        lines = []
        
        for metric in self.collect_all():
            # Build labels string
            if metric.labels:
                label_str = ",".join(
                    f'{k}="{v}"' for k, v in metric.labels.items()
                )
                lines.append(f"{metric.name}{{{label_str}}} {metric.value}")
            else:
                lines.append(f"{metric.name} {metric.value}")
        
        return "\n".join(lines)
    
    def export_json(self) -> dict:
        """Export metrics as JSON."""
        return {
            "metrics": [
                {
                    "name": m.name,
                    "value": m.value,
                    "labels": m.labels,
                    "timestamp": m.timestamp
                }
                for m in self.collect_all()
            ],
            "exported_at": time.time()
        }


# =============================================================================
# Pre-defined Sync Metrics
# =============================================================================

# Global registry
_registry = MetricsRegistry()


# Sync operations
sync_operations_total = _registry.counter(
    "operations_total",
    "Total number of sync operations processed",
    labels=["operation", "status"]
)

sync_conflicts_total = _registry.counter(
    "conflicts_total",
    "Total number of sync conflicts detected",
    labels=["resolution"]
)

# Performance
sync_latency_seconds = _registry.histogram(
    "latency_seconds",
    "Sync operation latency in seconds",
    labels=["operation"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf'))
)

bundle_size_bytes = _registry.histogram(
    "bundle_size_bytes",
    "Size of sync bundles in bytes",
    labels=["direction"],
    buckets=(1024, 10240, 102400, 1048576, 10485760, float('inf'))
)

# Connections
active_connections = _registry.gauge(
    "active_connections",
    "Number of active sync connections",
    labels=["transport"]
)

# Health
last_sync_timestamp = _registry.gauge(
    "last_sync_timestamp",
    "Timestamp of last successful sync",
    labels=["device_id"]
)


def get_registry() -> MetricsRegistry:
    """Get the global metrics registry."""
    return _registry


# =============================================================================
# Structured Logging
# =============================================================================

class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""
    
    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra
        self._hostname = os.environ.get("HOSTNAME", "unknown")
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "hostname": self._hostname
        }
        
        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        if self.include_extra:
            for key, value in record.__dict__.items():
                if key not in (
                    'name', 'msg', 'args', 'created', 'filename', 'funcName',
                    'levelname', 'levelno', 'lineno', 'module', 'msecs',
                    'pathname', 'process', 'processName', 'relativeCreated',
                    'stack_info', 'exc_info', 'exc_text', 'thread', 'threadName',
                    'message', 'taskName'
                ):
                    log_data[key] = value
        
        return json.dumps(log_data)


class SyncLogger:
    """
    Structured logger for sync operations.
    
    Provides convenience methods for common sync events.
    """
    
    def __init__(self, name: str = "sqlite_sync"):
        self._logger = logging.getLogger(name)
    
    def sync_started(
        self,
        device_id: str,
        peer_id: str = None,
        transport: str = None
    ) -> None:
        """Log sync start."""
        self._logger.info(
            "Sync started",
            extra={
                "event": "sync_started",
                "device_id": device_id,
                "peer_id": peer_id,
                "transport": transport
            }
        )
    
    def sync_completed(
        self,
        device_id: str,
        ops_sent: int,
        ops_received: int,
        duration_ms: float,
        conflicts: int = 0
    ) -> None:
        """Log sync completion."""
        self._logger.info(
            f"Sync completed: sent={ops_sent}, received={ops_received}, conflicts={conflicts}",
            extra={
                "event": "sync_completed",
                "device_id": device_id,
                "ops_sent": ops_sent,
                "ops_received": ops_received,
                "conflicts": conflicts,
                "duration_ms": duration_ms
            }
        )
        
        # Update metrics
        sync_operations_total.inc(ops_sent, operation="push", status="success")
        sync_operations_total.inc(ops_received, operation="pull", status="success")
        sync_conflicts_total.inc(conflicts, resolution="detected")
    
    def sync_failed(
        self,
        device_id: str,
        error: str,
        retry_count: int = 0
    ) -> None:
        """Log sync failure."""
        self._logger.error(
            f"Sync failed: {error}",
            extra={
                "event": "sync_failed",
                "device_id": device_id,
                "error": error,
                "retry_count": retry_count
            }
        )
        
        sync_operations_total.inc(1, operation="sync", status="failed")
    
    def conflict_detected(
        self,
        device_id: str,
        table_name: str,
        row_pk: str,
        resolution: str
    ) -> None:
        """Log conflict detection."""
        self._logger.warning(
            f"Conflict detected on {table_name}",
            extra={
                "event": "conflict_detected",
                "device_id": device_id,
                "table_name": table_name,
                "row_pk": row_pk,
                "resolution": resolution
            }
        )
        
        sync_conflicts_total.inc(1, resolution=resolution)
    
    def bundle_created(
        self,
        device_id: str,
        peer_id: str,
        size_bytes: int,
        op_count: int
    ) -> None:
        """Log bundle creation."""
        self._logger.info(
            f"Bundle created: {op_count} ops, {size_bytes} bytes",
            extra={
                "event": "bundle_created",
                "device_id": device_id,
                "peer_id": peer_id,
                "size_bytes": size_bytes,
                "op_count": op_count
            }
        )
        
        bundle_size_bytes.observe(size_bytes, direction="outbound")
    
    def bundle_imported(
        self,
        device_id: str,
        source_id: str,
        applied: int,
        skipped: int,
        conflicts: int
    ) -> None:
        """Log bundle import."""
        self._logger.info(
            f"Bundle imported: applied={applied}, skipped={skipped}, conflicts={conflicts}",
            extra={
                "event": "bundle_imported",
                "device_id": device_id,
                "source_id": source_id,
                "applied": applied,
                "skipped": skipped,
                "conflicts": conflicts
            }
        )


def configure_logging(
    level: str = "INFO",
    json_format: bool = True,
    log_file: str = None
) -> None:
    """
    Configure logging for production.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        json_format: Use JSON formatting
        log_file: Optional log file path
    """
    handlers = []
    
    # Console handler
    console = logging.StreamHandler()
    if json_format:
        console.setFormatter(JSONFormatter())
    else:
        console.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
    handlers.append(console)
    
    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JSONFormatter())
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        handlers=handlers
    )


# =============================================================================
# Health Checks
# =============================================================================

@dataclass
class HealthStatus:
    """Health check status."""
    healthy: bool
    checks: Dict[str, dict]
    timestamp: float = field(default_factory=time.time)


class HealthChecker:
    """
    Comprehensive health checker.
    
    Supports:
    - Database connectivity
    - Transport availability
    - Performance thresholds
    """
    
    def __init__(self):
        self._checks: Dict[str, Callable[[], dict]] = {}
    
    def register_check(
        self,
        name: str,
        check_fn: Callable[[], dict]
    ) -> None:
        """
        Register a health check.
        
        Check function should return:
        {"healthy": bool, "message": str, ...}
        """
        self._checks[name] = check_fn
    
    def check_all(self) -> HealthStatus:
        """Run all health checks."""
        results = {}
        all_healthy = True
        
        for name, check_fn in self._checks.items():
            try:
                result = check_fn()
                results[name] = result
                if not result.get("healthy", False):
                    all_healthy = False
            except Exception as e:
                results[name] = {
                    "healthy": False,
                    "message": f"Check failed: {str(e)}"
                }
                all_healthy = False
        
        return HealthStatus(
            healthy=all_healthy,
            checks=results
        )
    
    def check_database(self, conn) -> dict:
        """Check database connectivity and performance."""
        try:
            start = time.perf_counter()
            conn.execute("SELECT 1").fetchone()
            latency_ms = (time.perf_counter() - start) * 1000
            
            return {
                "healthy": True,
                "message": "Database connected",
                "latency_ms": latency_ms
            }
        except Exception as e:
            return {
                "healthy": False,
                "message": f"Database error: {str(e)}"
            }
    
    def check_memory(self, threshold_mb: int = 1000) -> dict:
        """Check memory usage."""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            return {
                "healthy": memory_mb < threshold_mb,
                "message": f"Memory usage: {memory_mb:.1f} MB",
                "memory_mb": memory_mb,
                "threshold_mb": threshold_mb
            }
        except ImportError:
            return {
                "healthy": True,
                "message": "psutil not available, skipping memory check"
            }
    
    def check_disk(self, path: str = ".", threshold_percent: int = 90) -> dict:
        """Check disk space."""
        try:
            import shutil
            total, used, free = shutil.disk_usage(path)
            used_percent = (used / total) * 100
            
            return {
                "healthy": used_percent < threshold_percent,
                "message": f"Disk usage: {used_percent:.1f}%",
                "used_percent": used_percent,
                "free_bytes": free
            }
        except Exception as e:
            return {
                "healthy": False,
                "message": f"Disk check failed: {str(e)}"
            }


# Global health checker
_health_checker = HealthChecker()


def get_health_checker() -> HealthChecker:
    """Get the global health checker."""
    return _health_checker
