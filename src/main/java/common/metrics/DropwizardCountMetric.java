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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

/**
 * Statistic that records the sum of the recorded value and resets when the metric is read.
 */
class DropwizardCountMetric extends AbstractMetric implements Metric {

  static class ResettingCounter extends Counter {

    @Override
    public long getCount() {
      long count = super.getCount();
      dec(count);
      return count;
    }
  }

  private final Counter counter;

  public DropwizardCountMetric(String id, MetricRegistry metricRegistry) {
    super(id);
    counter = metricRegistry.counter(id, ResettingCounter::new);
  }

  @Override
  protected void doRecord(long v) {
    counter.inc(v);
  }
}
