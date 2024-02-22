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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

/** Statistic that writes the per-second average of the recorded value. */
public class DropwizardAverageMetric extends AbstractMetric implements Metric {

  private final Histogram histogram;

  public DropwizardAverageMetric(String id, MetricRegistry metricRegistry) {
    super(id);
    histogram = metricRegistry.histogram(id);
  }

  @Override
  protected void doRecord(long v) {
    histogram.update(v);
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Unimplemented method 'reset'");
  }

  @Override
  public void ping() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'ping'");
  }
}
