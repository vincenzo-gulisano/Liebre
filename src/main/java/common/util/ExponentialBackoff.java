package common.util;

import static common.util.Util.sleep;

import java.util.Random;

public class ExponentialBackoff implements Backoff {

  private static final int INTEGER_RANGE_MAX_SHIFT = 30;
  private final int maxShift;
  private final long initialSleepMs;
  private final Random random = new Random();
  private int shift;
  private static final int MAX_RETRIES = 3;
  private int retries;

  public ExponentialBackoff(long initialSleepMs, int maxShift) {
    if (maxShift > INTEGER_RANGE_MAX_SHIFT) {
      throw new IllegalArgumentException(
          String.format("maxShift cannot be greater than %d", INTEGER_RANGE_MAX_SHIFT));
    }
    this.initialSleepMs = initialSleepMs;
    this.maxShift = maxShift;
  }

  @Override
  public void backoff() {
    shift = Math.min(shift + 1, maxShift);
    int multiplier = 1 + random.nextInt(1 << shift);
    sleep(initialSleepMs * multiplier);
  }

  @Override
  public void relax() {
    if (retries++ > MAX_RETRIES) {
      retries = 0;
      shift = Math.max(shift - 1, 0);
    }
  }


}
