package metrics.micrometer.loggingsink;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.loggingsink.InfluxLineProtocolLoggingSink;
import org.corfudb.common.metrics.micrometer.protocoltransformer.LineTransformer;
import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Java6Assertions.assertThat;

@Slf4j
public class InfluxLineProtocolLoggingSinkTest {

    @Test
    public void testGaugeParsing() {
        InfluxLineProtocolLoggingSink sink = new InfluxLineProtocolLoggingSink(log);
        LineTransformer lineTransformer = sink.getLineTransformer();
        String datapoint = "request_counter{endpoint=localhost:9000 server=base_server} value=3";
        Optional<String> transformedDataPoint = lineTransformer.transformLine(datapoint);
        String expectedSubstring = "request_counter,endpoint=localhost:9000 server=base_server value=3";
        assertThat(transformedDataPoint.isPresent()).isTrue();
        assertThat(transformedDataPoint.get()).contains(expectedSubstring);
        String datapoint2 = "request_counter{endpoint=localhost:9000 server=base_server} value=3.14";
        Optional<String> transformedDataPoint2 = lineTransformer.transformLine(datapoint2);
        assertThat(transformedDataPoint2.isPresent()).isTrue();
        String expectedSubstring2 = "request_counter,endpoint=localhost:9000 server=base_server value=3.14";
        assertThat(transformedDataPoint2.get()).contains(expectedSubstring2);
    }

    @Test
    public void testCounterParsing() {
        InfluxLineProtocolLoggingSink sink = new InfluxLineProtocolLoggingSink(log);
        LineTransformer lineTransformer = sink.getLineTransformer();
        String datapoint = "request_rate{endpoint=localhost:9000 server=base_server} throughput=3.456/s";
        Optional<String> transformedDataPoint = lineTransformer.transformLine(datapoint);
        String expectedSubstring = "request_rate,endpoint=localhost:9000 server=base_server throughput=3.456/s";
        assertThat(transformedDataPoint.isPresent()).isTrue();
        assertThat(transformedDataPoint.get()).contains(expectedSubstring);
    }

    @Test
    public void testTimerParsing() {
        InfluxLineProtocolLoggingSink sink = new InfluxLineProtocolLoggingSink(log);
        LineTransformer lineTransformer = sink.getLineTransformer();
        String datapoint = "request_rate{endpoint=localhost:9000 server=base_server} throughput=3.456/s mean=3.516s max=5.134s";
        Optional<String> transformedDataPoint = lineTransformer.transformLine(datapoint);
        String expectedSubstring = "request_rate,endpoint=localhost:9000 server=base_server throughput=3.456/s mean=3.516s max=5.134s";
        assertThat(transformedDataPoint.isPresent()).isTrue();
        assertThat(transformedDataPoint.get()).contains(expectedSubstring);
    }

    @Test
    public void testByteDistSummaryParsing() {
        InfluxLineProtocolLoggingSink sink = new InfluxLineProtocolLoggingSink(log);
        LineTransformer lineTransformer = sink.getLineTransformer();
        String datapoint = "request_rate{endpoint=localhost:9000 server=base_server} throughput=3.456/s mean=3.516 MiB max=1 GiB";
        Optional<String> transformedDataPoint = lineTransformer.transformLine(datapoint);
        String expectedSubstring = "request_rate,endpoint=localhost:9000 server=base_server throughput=3.456/s mean=3.516 MiB max=1 GiB";
        assertThat(transformedDataPoint.isPresent()).isTrue();
        assertThat(transformedDataPoint.get()).contains(expectedSubstring);
    }

    @Test
    public void longRunningTaskParsing() {
        InfluxLineProtocolLoggingSink sink = new InfluxLineProtocolLoggingSink(log);
        LineTransformer lineTransformer = sink.getLineTransformer();
        String datapoint = "lock_hold_duration{endpoint=localhost:9000 type=active} active=1 task duration=30s";
        Optional<String> transformedDataPoint = lineTransformer.transformLine(datapoint);
        String expectedSubstring = "lock_hold_duration,endpoint=localhost:9000 type=active active=1 task duration=30s";
        assertThat(transformedDataPoint.isPresent()).isTrue();
        assertThat(transformedDataPoint.get()).contains(expectedSubstring);
    }
}
