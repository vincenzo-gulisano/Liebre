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

package common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Sometimes it is useful to stop the whole system in case a processing thread crashes. This
 * exception handler achieves just this. When used and a thread throws an uncaught exception, the
 * whole JVM will stop to speed up bug detection and resolution.
 *
 * @author palivosd
 */
public enum StopJvmUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  INSTANCE;
  public static final int INTERRUPTED_EXIT_CODE = 130;
  private final Logger LOGGER = LogManager.getLogger();

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    if ((e instanceof InterruptedException) || (e.getCause() instanceof InterruptedException)) {
      LOGGER.warn("Interrupted", e);
      System.exit(INTERRUPTED_EXIT_CODE);
    }
    LOGGER.error("{} crashed", t, e);
    LOGGER.error("Details: {}", e);
    LOGGER.error("Shutting down");
    System.exit(1);
  }
}
