package common.util;

public enum NoopBackoff implements Backoff {
  INSTANCE;

  @Override
  public void backoff() {

  }

  @Override
  public void relax() {

  }
}
