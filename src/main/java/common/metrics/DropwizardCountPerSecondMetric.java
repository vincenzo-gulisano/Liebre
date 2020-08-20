/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package common.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/** Statistic that records the per-second sum of the recorded value. */
class DropwizardCountPerSecondMetric extends AbstractMetric implements Metric {

  private final AverageGauge gauge;

  private static class AverageGauge implements Gauge<Long> {
    private long startTime = System.currentTimeMillis();
    private long sum = 0;

    void add(long value) {
      synchronized (this) {
        sum += value;
      }
    }

    @Override
    public Long getValue() {
      long newTime = System.currentTimeMillis();
      long value = (1000 * sum) / (newTime - startTime);
      synchronized (this) {
        startTime = newTime;
        sum = 0;
      }
      return value;
    }
  }

  public DropwizardCountPerSecondMetric(String id, MetricRegistry metricRegistry) {
    super(id);
    gauge = (AverageGauge) metricRegistry.gauge(id, AverageGauge::new);
  }

  @Override
  protected void doRecord(long v) {
    gauge.add(v);
  }
}
