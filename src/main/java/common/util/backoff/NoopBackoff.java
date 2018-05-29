package common.util.backoff;

public enum NoopBackoff implements Backoff {
  INSTANCE;

  @Override
  public Backoff newInstance() {
    return INSTANCE;
  }

  @Override
  public void backoff() {

  }

  @Override
  public void relax() {

  }
}
