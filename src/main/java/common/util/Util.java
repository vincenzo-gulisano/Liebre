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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * General utilities
 */
public class Util {

  private static final Logger LOGGER = LogManager.getLogger();

  public static volatile long LOOPS_PER_MILLI = 100000;

  private Util() {

  }

  /**
   * Sleep for the specified amount of time. This function catches any {@link
   * InterruptedException}s, logs the important part of the stack trace and sets {@code
   * Thread.currentThread().interrupt()}. <br/>
   *
   * <emph>Note: Be careful when using this function because it will reset the interrupted
   * status of the thread. In many cases this is a good default. However, in case you want to clear
   * that, please use {@link Thread#sleep(long)} and handle any {@link InterruptedException}s
   * manually* </emph>
   *
   * @param millis The time to sleep, in milliseconds.
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOGGER.debug("Sleep interrupted: {}", e.getStackTrace()[2]);
      Thread.currentThread().interrupt();
    }
  }
  
  @SafeVarargs
public static <T> List<T> makeList(T ... params) {
	  ArrayList<T> result = new ArrayList<T>(params.length);
	  	for (T p : params)
	  		result.add(p);
	  return result;
  }

}
