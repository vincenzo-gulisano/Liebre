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

/** Statistic that writes the per-second average of the recorded value. */
public class FileAverageMetric extends AbstractFileMetric {

  private long sum;
  private long count;
  private long prevSec;

  public FileAverageMetric(String id, String folder, boolean autoFlush) {
    super(id, folder, autoFlush);
  }

  @Override
  protected void doRecord(long v) {
    writePreviousAverages();
    sum += v;
    count++;
  }

  @Override
  public void enable() {
    this.sum = 0;
    this.count = 0;
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
      long average = (count != 0 ? sum / count : -1);
      writeCSVLine(prevSec, average);
      sum = 0;
      count = 0;
      prevSec++;
    }
  }

  @Override
  public void reset() {
    sum = 0;
    count = 0;
  }
}
