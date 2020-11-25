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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

/**
 * Statistic that writes the per-second average of the recorded value.
 */
public class DropwizardAverageMetric extends AbstractMetric implements Metric {

  private final AverageGauge gauge;

  public DropwizardAverageMetric(String id, MetricRegistry metricRegistry) {
    super(id);
    gauge = new AverageGauge();
    metricRegistry.register(id, this.gauge);
  }

  @Override
  protected void doRecord(long v) {
    gauge.add(v);
  }

  private static class AverageGauge implements Gauge {

    private double sum;
    private int count;

    public void add(double value) {
      sum += value;
      count += 1;
    }

    @Override
    public Object getValue() {
      double result = sum / count;
      sum = 0;
      count = 0;
      return result;
    }
  }
}