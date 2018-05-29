package common.util;

public interface Backoff {

  void backoff();

  void relax();
}
