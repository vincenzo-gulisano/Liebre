package common.util;

public class RateLimiter {

	private final long intervalMillis;
	private volatile long lastInterval;
	private volatile long callsNumber;

	/**
	 * Construct with a default interval of {@code 100} ms.
	 */
	public RateLimiter() {
		this(100);
	}

	/**
	 * Construct with the given custom interval
	 * 
	 * @param intervalMillis
	 *            The interval where the limit measurements take place, i.e. the
	 *            time "resolution" of the limiter.
	 */
	public RateLimiter(long intervalMillis) {
		this.intervalMillis = intervalMillis;
		this.lastInterval = System.currentTimeMillis() / intervalMillis;
	}

	public void limit(long convertedRate) {
		if (updateInterval()) {
			return;
		}
		if (++callsNumber >= convertedRate) {
			waitUntilNextInterval();
		}
	}

	/**
	 * Convert tuples/second to tuples/interval
	 * 
	 * @param rate
	 *            The rate in tuples/second
	 * @return The rate in tuples/interval
	 */
	public long getConvetedRate(long rate) {
		return (intervalMillis * rate) / 1000;
	}

	private boolean updateInterval() {
		long currentInterval = System.currentTimeMillis() / intervalMillis;
		if (lastInterval == currentInterval) {
			return false;
		} else {
			lastInterval = currentInterval;
			callsNumber = 0;
			return true;
		}
	}

	/**
	 * Wait until the next interval. When this function returns, the interval will
	 * have been updated to a new value and the calls will be reset to 0.
	 */
	private void waitUntilNextInterval() {
		while (!updateInterval()) {
			long millisToSleep = millisUntilNextInterval();
			if (millisToSleep > 0) {
				Util.sleep(millisToSleep);
			}
		}
	}

	private long millisUntilNextInterval() {
		return (lastInterval + 1) * intervalMillis - System.currentTimeMillis();
	}
}
