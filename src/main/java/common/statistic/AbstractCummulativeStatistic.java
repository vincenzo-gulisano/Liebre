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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for implementing cummulative statistics that record values at arbitrary intervals
 * and write summaries in a csv file.
 *
 * @author palivosd
 */
public abstract class AbstractCummulativeStatistic implements Active {

  private static final Logger LOG = LogManager.getLogger();
  private static final Pattern EXTRACT_METRIC_NAME = Pattern.compile(".+\\/(.+)\\.csv");
  protected final String metricName;
  private boolean enabled;

  public AbstractCummulativeStatistic(String outputFile, boolean autoFlush) {
    final Matcher m = EXTRACT_METRIC_NAME.matcher(outputFile);
    if (m.matches()) {
      metricName = m.group(1);
    } else {
      throw new IllegalStateException(String.format("Failed to extract metric name from path: %s",
          outputFile));
    }
  }

  public final void append(long value) {
    if (isEnabled()) {
      doAppend(value);
    }
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
    if (!isEnabled()) {
      LOG.warn("Disabling {} which was already disabled!", metricName);
    }
    this.enabled = false;
  }

  protected abstract void doAppend(long value);
}
