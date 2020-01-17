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

public final class LiebreContext {

  private static MetricsFactory operatorMetricsFactory = new InactiveMetricsFactory();

  private static MetricsFactory streamMetricsFactory = new InactiveMetricsFactory();

  private static MetricsFactory userMetricsFactory = new InactiveMetricsFactory();

  public static MetricsFactory operatorMetricsFactory() {
    return operatorMetricsFactory;
  }

  public static MetricsFactory streamMetricsFactory() {
    return streamMetricsFactory;
  }

  public static MetricsFactory userMetricsFactory() {
    return userMetricsFactory;
  }

  public static void setOperatorMetricsFactory(MetricsFactory operatorMetricsFactory) {
    LiebreContext.operatorMetricsFactory = operatorMetricsFactory;
  }

  public static void setStreamMetricsFactory(MetricsFactory streamMetricsFactory) {
    LiebreContext.streamMetricsFactory = streamMetricsFactory;
  }

  public static void setUserMetricsFactory(MetricsFactory userMetricsFactory) {
    LiebreContext.userMetricsFactory = userMetricsFactory;
  }

  private LiebreContext() {}
}
