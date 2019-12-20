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

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.palyvos.liebre.statistics.StatisticsFactory;

public final class LiebreContext {

  private static final MetricRegistry registry = new MetricRegistry();

  @Inject
  @Named("operator")
  private static StatisticsFactory operatorStatisticsFactory;

  @Inject
  @Named("stream")
  private static StatisticsFactory streamStatisticsFactory;

  @Inject
  @Named("user")
  private static StatisticsFactory userStatisticsFactory;

  private LiebreContext() {}

  public static MetricRegistry registry() {
    return registry;
  }

  public static StatisticsFactory operatorStatistiscFactory() {
    return operatorStatisticsFactory;
  }

  public static StatisticsFactory streamStatisticsFactory() {
    return streamStatisticsFactory;
  }

  public static StatisticsFactory userStatisticsFactory() {
    return userStatisticsFactory;
  }
}
