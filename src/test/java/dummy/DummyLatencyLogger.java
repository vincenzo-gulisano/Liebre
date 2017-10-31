package dummy;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import sink.SinkFunction;

/**
 * Log average latency per second, in milliseconds.
 * 
 * @author palivosd
 *
 */
public class DummyLatencyLogger implements SinkFunction<DummyTuple> {

	private double latencyMeasumentsCount = 0;
	private double latencySumNanoseconds = 0;
	private long lastUpdateTimeSeconds = currentTimeSeconds();
	private static final double NANO_TO_MILLI = Math.pow(10, 6);

	private final PrintWriter pw;

	public DummyLatencyLogger(String fileName) {
		try {
			this.pw = new PrintWriter(new FileWriter(fileName), true);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Object processTuple(DummyTuple tuple) {
		latencyMeasumentsCount++;
		latencySumNanoseconds += System.nanoTime() - tuple.getTimestamp();
		while (currentTimeSeconds() - lastUpdateTimeSeconds > 1) {
			pw.format("%d,%3.0f%n", lastUpdateTimeSeconds, latencyMilliseconds());
			latencyMeasumentsCount = 0;
			latencySumNanoseconds = 0;
			lastUpdateTimeSeconds++;
		}
		return tuple;
	}

	private long currentTimeSeconds() {
		return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
	}

	private double latencyMilliseconds() {
		if (latencyMeasumentsCount > 0) {
			return latencySumNanoseconds / (latencyMeasumentsCount * NANO_TO_MILLI);
		}
		return Double.NaN;
	}
}