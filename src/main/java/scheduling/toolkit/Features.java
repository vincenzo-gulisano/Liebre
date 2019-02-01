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

package scheduling.toolkit;

public final class Features {

  private static int FEATURES_NUMBER = 5;

  public static int F_TOPOLOGICAL_ORDER = 0;
  public static int F_SELECTIVITY = 1;
  public static int F_COST = 2;
  public static int F_HEAD_ARRIVAL_TIME = 3;
  public static int F_COMPONENT_TYPE = 4;

  public static int CTYPE_SOURCE = 0;
  public static int CTYPE_SINK = 1;
  public static int CTYPE_OPERATOR = 2;
  public static int CTYPE_ROUTER = 3;
  public static int CTYPE_UNION = 4;
  public static int CTYPE_JOIN = 5;

  public static double getLatency(double arrivalTime, long currentTime) {
    return (arrivalTime < 0) ? 0 : currentTime - arrivalTime;
  }

  public static double getHeadLatency(double[] features, long currentTime) {
    return getLatency(features[F_HEAD_ARRIVAL_TIME], currentTime);
  }

  public static double[] create() {
    return new double[FEATURES_NUMBER];
  }

  private Features() {
  }

}
