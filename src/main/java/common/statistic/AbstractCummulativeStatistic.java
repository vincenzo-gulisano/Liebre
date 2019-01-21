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

import common.Active;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for implementing cummulative statistics that record values at arbitrary intervals
 * and write summaries in a csv file.
 *
 * @author palivosd
 */
public abstract class AbstractCummulativeStatistic implements Active {

  private final PrintWriter out;
  private volatile boolean enabled;
  private final Logger LOGGER = LogManager.getLogger();

  public AbstractCummulativeStatistic(String outputFile, boolean autoFlush) {
    try {
      FileWriter outFile = new FileWriter(outputFile);
      out = new PrintWriter(outFile, autoFlush);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Failed to open file %s for writing: %s", outputFile, e.getMessage()), e);
    }
  }

  protected void writeCommaSeparatedValues(Object... values) {
    if (!isEnabled()) {
      LOGGER.debug("Ignoring append, statistic is disabled");
      return;
    }
    StringBuilder sb = new StringBuilder();
    for (Object value : values) {
      sb.append(value).append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    writeLine(sb.toString());
  }

  protected void writeLine(String line) {
    if (!isEnabled()) {
      LOGGER.debug("Ignoring append, statistic is disabled");
      return;
    }
    out.println(line);
  }

  protected long currentTimeSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
  }

  @Override
  public void enable() {
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return this.enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    out.flush();
    out.close();
  }

  public final void append(long value) {
    if (!isEnabled()) {
      LOGGER.debug("Ignoring append, statistic is disabled");
      return;
    }
    doAppend(value);
  }

  protected abstract void doAppend(long value);
}
