package common.metrics;

public class DelegatingTimeMetric extends AbstractTimeMetric {

  private final Metric delegate;

  public DelegatingTimeMetric(Metric delegate) {
    super(delegate.id());
    this.delegate = delegate;
  }

  @Override
  public void enable() {
    delegate.enable();
    super.enable();
  }

  @Override
  public void disable() {
    super.disable();
    delegate.disable();
  }

  @Override
  public void record(long interval) {
    delegate.record(interval);
  }

  @Override
  public void reset() {
    delegate.reset();
  }

}
