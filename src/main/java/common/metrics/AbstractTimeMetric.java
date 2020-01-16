package common.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractTimeMetric implements TimeMetric {
  private static final Logger LOG = LogManager.getLogger();
  protected final String id;
  private boolean enabled;
  private long startTime = 0;

  public AbstractTimeMetric(String id) {
    this.id = id;
  }

  @Override
  public void startInterval() {
    if (!enabled) {
      return;
    }
    startTime = System.nanoTime();
  }

  @Override
  public void stopInterval() {
    if (!enabled) {
      return;
    }
    record(System.nanoTime() - startTime);
    startTime = 0;
  }

  @Override
  public void enable() {
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    if (!enabled) {
      LOG.warn("Disabling metric {} which was already disabled!", id);
    }
    this.enabled = false;
  }

  @Override
  public String id() {
    return id;
  }
}
