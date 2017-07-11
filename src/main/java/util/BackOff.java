package util;

import java.util.Random;

public class BackOff {

	final int min, max, retries;
	int currentLimit, currentRetries;
	Random rand;

	public BackOff(int min, int max, int retries) {
		this.min = min;
		this.max = max;
		this.retries = retries;
		rand = new Random();
		currentLimit = min;
		currentRetries = retries;
	}

	public int backoff() {

		int delay = rand.nextInt(currentLimit);
		currentRetries--;
		if (currentRetries == 0) {
			currentLimit = (2 * currentLimit < max) ? 2 * currentLimit : max;
			currentRetries = retries;
		}

		Util.sleep(delay);
		return delay;
	}

}
