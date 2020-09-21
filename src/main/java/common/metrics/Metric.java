package common.metrics;

import common.Active;

public interface Metric extends Active {

  void record(long value);

  String id();
}
