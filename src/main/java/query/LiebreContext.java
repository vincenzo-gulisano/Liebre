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

package query;

import common.metrics.InactiveMetricsFactory;
import common.metrics.MetricsFactory;
import component.sink.Sink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class LiebreContext {

  private static MetricsFactory operatorMetrics = new InactiveMetricsFactory();

  private static MetricsFactory streamMetrics = new InactiveMetricsFactory();

  private static MetricsFactory userMetrics = new InactiveMetricsFactory();

  private static boolean flushingEnabled = true;

  private static QueryTerminator queryTerminator = new QueryTerminator();

  /**
   * Sets whether the QueryTerminator should assume a single query execution. It
   * does so by default.
   * 
   * @param singleQueryExecution
   */
  public static void setSingleQueryExecution(boolean singleQueryExecution) {
    queryTerminator.setSingleQueryExecution(singleQueryExecution);
  }

  public static MetricsFactory operatorMetrics() {
    return operatorMetrics;
  }

  public static MetricsFactory streamMetrics() {
    return streamMetrics;
  }

  public static MetricsFactory userMetrics() {
    return userMetrics;
  }

  public static void setOperatorMetrics(MetricsFactory operatorMetrics) {
    LiebreContext.operatorMetrics = operatorMetrics;
  }

  public static void setStreamMetrics(MetricsFactory streamMetrics) {
    LiebreContext.streamMetrics = streamMetrics;
  }

  public static void setUserMetrics(MetricsFactory userMetrics) {
    LiebreContext.userMetrics = userMetrics;
  }

  public static void mergeWithOperatorMetrics(MetricsFactory operatorMetrics) {
    LiebreContext.operatorMetrics.mergeWith(operatorMetrics);
  }

  public static void mergeWithStreamMetrics(MetricsFactory streamMetrics) {
    LiebreContext.streamMetrics.mergeWith(streamMetrics);
  }

  public static void mergeWithUserMetrics(MetricsFactory userMetrics) {
    LiebreContext.userMetrics.mergeWith(userMetrics);
  }

  public static void unmergeFromOperatorMetrics(MetricsFactory operatorMetrics) {
    LiebreContext.operatorMetrics.unmergeFrom(operatorMetrics);
  }

  public static void unmergeFromStreamMetrics(MetricsFactory streamMetrics) {
    LiebreContext.streamMetrics.unmergeFrom(streamMetrics);
  }

  public static void unmergeFromUserMetrics(MetricsFactory userMetrics) {
    LiebreContext.userMetrics.unmergeFrom(userMetrics);
  }

  public static void init(Query query) {
    queryTerminator.registerQuery(query);
    if (!queryTerminator.hasBeenActivated()) {
      queryTerminator.activate();
    }
  }

  public static void terminated(Query query) {
    queryTerminator.deregisterQuery(query);
  }

  public static void sinkFinished(Query query, Sink<?> sink) {
    queryTerminator.sinkFinished(query, sink);
  }

  public static boolean isFlushingEnabled() {
    return flushingEnabled;
  }

  public static void interruptTerminator() {
    queryTerminator.interruptTerminator();
  }

  public static void disableFlushing() {
    flushingEnabled = false;
  }

  private LiebreContext() {
  }

}
