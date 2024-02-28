package common.metrics;
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

import java.util.function.Consumer;

/** Statistic that writes the per-second average of the recorded value. */
public class FileAndConsumerAverageMetric extends AbstractFileAndConsumerMetric {

  private long sum;
  private long count;
  private long prevSec;

  private final long missingValue = -1;
  private final long neutralValue = 0;

  public FileAndConsumerAverageMetric(String id, String folder, boolean autoFlush, Consumer<Object[]> c) {
    super(id, folder, autoFlush, c);
  }

  @Override
  protected void doRecord(long v) {
    writePreviousAverages();
    if (sum==missingValue) {
      sum=neutralValue;
    }
    if (count==missingValue) {
      count=neutralValue;
    }
    sum += v;
    count++;
  }

  @Override
  public void enable() {
    this.sum = missingValue;
    this.count = missingValue;
    prevSec = currentTimeSeconds();
    super.enable();
  }

  public void disable() {
    writePreviousAverages();
    super.disable();
  }

  private void writePreviousAverages() {
    long thisSec = currentTimeSeconds();
    while (prevSec < thisSec) {
      long average = (count != missingValue ? sum / count : missingValue);
      writeCSVLineAndConsume(prevSec, average);
      sum = missingValue;
      count = missingValue;
      prevSec++;
    }
  }

  @Override
  public void reset() {
    sum = missingValue;
    count = missingValue;
  }

  @Override
  public void ping() {
    writePreviousAverages();
  }
}
