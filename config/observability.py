"""
Observability setup for SignOz/OpenTelemetry.
Extract setup_tracing, setup_metrics from incremental_utils.py
"""

import logging
from typing import Optional
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from config.settings import SIGNOZ_ENDPOINT, OTEL_SERVICE_NAME, ENABLE_TRACING, ENABLE_METRICS

logger = logging.getLogger(__name__)


class ObservabilityManager:
    """Manages OpenTelemetry tracing and metrics."""

    _tracer_provider: Optional[TracerProvider] = None
    _meter_provider: Optional[MeterProvider] = None

    @classmethod
    def setup_tracing(cls) -> None:
        """Setup OpenTelemetry tracing with SignOz."""
        if not ENABLE_TRACING:
            logger.info("Tracing is disabled")
            return

        if cls._tracer_provider is not None:
            logger.info("Tracing already initialized")
            return

        try:
            # Create resource
            resource = Resource(attributes={
                SERVICE_NAME: OTEL_SERVICE_NAME
            })

            # Create tracer provider
            cls._tracer_provider = TracerProvider(resource=resource)

            # Create OTLP exporter
            otlp_exporter = OTLPSpanExporter(endpoint=SIGNOZ_ENDPOINT, insecure=True)

            # Add span processor
            span_processor = BatchSpanProcessor(otlp_exporter)
            cls._tracer_provider.add_span_processor(span_processor)

            # Set global tracer provider
            trace.set_tracer_provider(cls._tracer_provider)

            logger.info(f"Tracing initialized: {SIGNOZ_ENDPOINT}")
        except Exception as e:
            logger.error(f"Failed to initialize tracing: {e}")

    @classmethod
    def setup_metrics(cls) -> None:
        """Setup OpenTelemetry metrics with SignOz."""
        if not ENABLE_METRICS:
            logger.info("Metrics are disabled")
            return

        if cls._meter_provider is not None:
            logger.info("Metrics already initialized")
            return

        try:
            # Create resource
            resource = Resource(attributes={
                SERVICE_NAME: OTEL_SERVICE_NAME
            })

            # Create OTLP exporter
            otlp_exporter = OTLPMetricExporter(endpoint=SIGNOZ_ENDPOINT, insecure=True)

            # Create metric reader
            metric_reader = PeriodicExportingMetricReader(
                exporter=otlp_exporter,
                export_interval_millis=60000  # Export every 60 seconds
            )

            # Create meter provider
            cls._meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[metric_reader]
            )

            # Set global meter provider
            metrics.set_meter_provider(cls._meter_provider)

            logger.info(f"Metrics initialized: {SIGNOZ_ENDPOINT}")
        except Exception as e:
            logger.error(f"Failed to initialize metrics: {e}")

    @classmethod
    def get_tracer(cls, name: str):
        """Get tracer for instrumenting code."""
        if cls._tracer_provider is None:
            cls.setup_tracing()
        return trace.get_tracer(name)

    @classmethod
    def get_meter(cls, name: str):
        """Get meter for recording metrics."""
        if cls._meter_provider is None:
            cls.setup_metrics()
        return metrics.get_meter(name)

    @classmethod
    def shutdown(cls) -> None:
        """Shutdown tracing and metrics."""
        if cls._tracer_provider is not None:
            cls._tracer_provider.shutdown()
            cls._tracer_provider = None
            logger.info("Tracing shutdown")

        if cls._meter_provider is not None:
            cls._meter_provider.shutdown()
            cls._meter_provider = None
            logger.info("Metrics shutdown")


# Convenience functions
def setup_tracing() -> None:
    """Initialize tracing."""
    ObservabilityManager.setup_tracing()


def setup_metrics() -> None:
    """Initialize metrics."""
    ObservabilityManager.setup_metrics()


def get_tracer(name: str):
    """Get tracer."""
    return ObservabilityManager.get_tracer(name)


def get_meter(name: str):
    """Get meter."""
    return ObservabilityManager.get_meter(name)


def initialize_observability() -> None:
    """Initialize both tracing and metrics."""
    setup_tracing()
    setup_metrics()
    logger.info("Observability initialized")
