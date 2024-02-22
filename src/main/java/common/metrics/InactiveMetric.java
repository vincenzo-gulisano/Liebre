package common.metrics;

// Statistic implementation for instances where a statistic is not needed
public enum InactiveMetric implements Metric, TimeMetric {
  INSTANCE;

  public static final String INACTIVE = "Inactive";

  @Override
  public void startInterval() {
  }

  @Override
  public void stopInterval() {
  }

  @Override
  public void record(long value) {
  }

  @Override
  public String id() {
    return INACTIVE;
  }

  @Override
  public void enable() {
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public void disable() {
  }

  @Override
  public void reset() {
  }

  @Override
  public void ping() {
    
  }
}
