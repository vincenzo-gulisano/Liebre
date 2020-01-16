package common.metrics;

import org.apache.commons.lang3.Validate;

public class DefaultMetricName implements MetricName {

  @Override
  public String get(String id, Object type) {
    Validate.notBlank(id, "id");
    Validate.notNull(type);
    Validate.notBlank(type.toString());
    return String.format("%s.%s", id, type.toString());
  }
}
