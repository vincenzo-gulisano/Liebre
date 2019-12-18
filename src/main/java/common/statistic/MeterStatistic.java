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

package common.statistic;

import com.codahale.metrics.Gauge;

/** Statistic that records the per-second sum of the recorded value. */
public class MeterStatistic extends AbstractCummulativeStatistic {

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

  public MeterStatistic(String outputFile, boolean autoFlush) {
    super(outputFile, autoFlush);
    gauge = (AverageGauge) LiebreMetricRegistry.get().gauge(metricName, AverageGauge::new);
  }

  @Override
  protected void doAppend(long v) {
    gauge.add(v);
  }
}
