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
import common.util.Util;
import component.operator.in1.aggregate.BaseTimeBasedSingleWindow;
import component.sink.AbstractSink;
import component.sink.Sink;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class LiebreContext {

  private static final Logger LOG = LogManager.getLogger();

  private static MetricsFactory operatorMetrics = new InactiveMetricsFactory();

  private static MetricsFactory streamMetrics = new InactiveMetricsFactory();

  private static MetricsFactory userMetrics = new InactiveMetricsFactory();

  private static Query activeQuery;

  private static final Set<String> activeSinks = ConcurrentHashMap.newKeySet();

  private static Runnable terminator = new Runnable() {
    @Override
    public void run() {
      LOG.info("Terminator started");
      while (!activeSinks.isEmpty()) {
        LOG.info("Active Sinks: {}", activeSinks);
        Util.sleep(10000);
      }
      activeQuery.deActivate();
    }
  };

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

  public static void init(Query query) {
    activeQuery = query;
    activeSinks.addAll(query.sinks().stream().map(s -> s.getId()).collect(Collectors.toList()));
    new Thread(terminator).start();
  }

  public static void sinkFinished(Sink<?> sink) {
    activeSinks.remove(sink.getId());
  }

  private LiebreContext() {
  }

}
