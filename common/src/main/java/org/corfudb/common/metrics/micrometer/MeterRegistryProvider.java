package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import org.corfudb.common.metrics.micrometer.loggingsink.InfluxLineProtocolLoggingSink;
import org.corfudb.common.metrics.micrometer.loggingsink.LoggingSink;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A configuration class for a meter (metrics) registry.
 */
public class MeterRegistryProvider {
    private static Optional<MeterRegistry> meterRegistry = Optional.empty();
    private static Optional<String> endpoint = Optional.empty();
    private MeterRegistryProvider() {

    }

    /**
     * Class that initializes the Meter Registry.
     */
    public static class MeterRegistryInitializer extends MeterRegistryProvider {

        /**
         * Create a new instance of MeterRegistry with the given logger, loggingInterval
         * and clientId.
         * @param logger A configured logger.
         * @param loggingInterval A duration between log appends for every metric.
         * @param clientId An id of a client for this metric.
         * @return A new meter registry.
         */
        public static MeterRegistry newInstance(Logger logger, Duration loggingInterval,
                                                UUID clientId) {
            LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
            InfluxLineProtocolLoggingSink sink =
                    new InfluxLineProtocolLoggingSink(logger);
            LoggingMeterRegistry registry = LoggingMeterRegistry.builder(config)
                    .loggingSink(sink).build();
            registry.config().commonTags("clientId", clientId.toString());
            return registry;
        }

        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported in the InfluxDB line protocol format
         * (https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
         * with  the provided loggingInterval frequency.
         * @param logger A configured logger.
         * @param loggingInterval A duration between log appends for every metric.
         * @param localEndpoint A local endpoint to tag every metric with.
         */
        public static synchronized void init(Logger logger, Duration loggingInterval,
                                                      String localEndpoint) {
            InfluxLineProtocolLoggingSink influxLineProtocolLoggingSink =
                    new InfluxLineProtocolLoggingSink(logger);
            init(loggingInterval, localEndpoint, influxLineProtocolLoggingSink);
        }

        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported in the InfluxDB line protocol format
         * (https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
         * with  the provided loggingInterval frequency.
         * @param logger A configured logger.
         * @param loggingInterval A duration between log appends for every metric.
         */
        public static synchronized void init(Logger logger, Duration loggingInterval) {
            InfluxLineProtocolLoggingSink influxLineProtocolLoggingSink =
                    new InfluxLineProtocolLoggingSink(logger);
            init(loggingInterval, influxLineProtocolLoggingSink);
        }



        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported via provided logging sink with
         * the provided loggingInterval frequency.
         * @param sink A configured logging sink.
         * @param loggingInterval A duration between log appends for every metric.
         * @param localEndpoint A local endpoint to tag every metric with.
         */
        public static synchronized void init(Duration loggingInterval,
                                                      String localEndpoint,
                                                      LoggingSink sink) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistry registry = LoggingMeterRegistry.builder(config)
                        .loggingSink(sink).build();
                registry.config().commonTags("endpoint", localEndpoint);
                endpoint = Optional.of(localEndpoint);
                return Optional.of(registry);
            };

            init(supplier);
        }

        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported via provided logging sink with
         * the provided loggingInterval frequency.
         * @param sink A configured logging sink.
         * @param loggingInterval A duration between log appends for every metric.
         */
        public static synchronized void init(Duration loggingInterval,
                                             LoggingSink sink) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistry registry = LoggingMeterRegistry.builder(config)
                        .loggingSink(sink).build();
                return Optional.of(registry);
            };

            init(supplier);
        }

        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported via provided logging sink with
         * the provided loggingInterval frequency.
         * @param sink A configured logging sink.
         * @param loggingInterval A duration between log appends for every metric.
         */
        public static synchronized void init(Duration loggingInterval,
                                             LoggingSink sink) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistry registry = LoggingMeterRegistry.builder(config)
                        .loggingSink(sink).build();
                return Optional.of(registry);
            };

            init(supplier);
        }

        /**
         * Configure the default registry of type LoggingMeterRegistry.
         */
        public static synchronized void init() {
            Supplier<Optional<MeterRegistry>> supplier = () -> Optional.of(new LoggingMeterRegistry());
            init(supplier);
        }

        private static void init(Supplier<Optional<MeterRegistry>> meterRegistrySupplier) {
            if (meterRegistry.isPresent()) {
                throw new IllegalStateException("Registry has already been initialized.");
            }
            meterRegistry = meterRegistrySupplier.get();
        }
    }

    /**
     * Get the previously configured meter registry.
     * If the registry has not been previously configured, return an empty option.
     * @return An optional configured meter registry.
     */
    public static synchronized Optional<MeterRegistry> getInstance() {
        return meterRegistry;
    }

    /**
     * Deregister the previously registered server meter.
     *
     * @param name Name of a meter.
     * @param tags Tags of the meter.
     * @param type Type of the meter.
     */
    public static synchronized void deregisterServerMeter(String name, Tags tags, Meter.Type type) {
        meterRegistry.ifPresent(registry -> {
            if (!endpoint.isPresent()) {
                throw new IllegalStateException("The endpoint must be known to deregister a server meter.");
            }
            String localEndpoint = endpoint.get();
            Meter.Id id = new Meter.Id(name, Tags.of("endpoint", localEndpoint).and(tags),
                    null, null, type);
            registry.remove(id);
        });
    }
}
