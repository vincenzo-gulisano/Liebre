package common.metrics;

import io.palyvos.dcs.common.Active;

public interface Metric extends Active {

  void record(long value);

  String id();
}
