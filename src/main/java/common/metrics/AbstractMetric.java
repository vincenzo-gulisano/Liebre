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

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for implementing metrics that record values over time.
 *
 * @author palivosd
 */
public abstract class AbstractMetric implements Metric {

  private static final Logger LOG = LogManager.getLogger();
  protected final String id;
  private boolean enabled;

  public AbstractMetric(String id) {
    this.id = id;
  }

  public final void record(long value) {
    if (isEnabled()) {
      doRecord(value);
    }
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
    if (!isEnabled()) {
      LOG.warn("Disabling metric {} which was already disabled!", id);
    }
    this.enabled = false;
  }

  @Override
  public String id() {
    return id;
  }

  protected abstract void doRecord(long value);
}
