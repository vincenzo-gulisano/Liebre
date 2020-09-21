package common.metrics;

import common.Active;

public interface TimeMetric extends Active {

  void startInterval();

  void stopInterval();

  void record(long interval);

  String id();
}
