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
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.concurrent.TimeUnit;

/**
 * Statistic that writes the per-second average of the recorded value.
 */
public class DropwizardSamplingHistorgramMetric extends AbstractMetric implements Metric {

  private final Histogram histogram;

  private final long sampleEvery = Metrics.sampleEvery();
  private long counter;

  public DropwizardSamplingHistorgramMetric(String id, MetricRegistry metricRegistry) {
    super(id);
    histogram = new Histogram(new SlidingTimeWindowArrayReservoir(1, TimeUnit.SECONDS));
    metricRegistry.register(id, histogram);
  }

  @Override
  protected void doRecord(long v) {
    if (sampleEvery < 0) {
      return;
    }
    if (counter++ % sampleEvery == 0) {
      histogram.update(v);
    }
  }

}
