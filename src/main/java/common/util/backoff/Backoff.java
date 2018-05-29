package common.util.backoff;

public interface Backoff {

  Backoff newInstance();

  void backoff();

  void relax();
}
