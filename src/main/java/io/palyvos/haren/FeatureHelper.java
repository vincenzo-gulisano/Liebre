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

package io.palyvos.haren;

import query.Query;

/**
 * Utility class that assigns unique IDs to each feature type and provides some sane defaults.
 */
public final class FeatureHelper {

  public static final double NO_ARRIVAL_TIME = Double.MAX_VALUE;
  public static final long MAX_QUEUE_SIZE = Query.DEFAULT_STREAM_CAPACITY;
  public static int CTYPE_SOURCE = 0;
  public static int CTYPE_SINK = 1;
  public static int CTYPE_OPERATOR = 2;
  public static int CTYPE_ROUTER = 3;
  public static int CTYPE_UNION = 4;
  public static int CTYPE_JOIN = 5;

  private FeatureHelper() {
  }

  public static double getLatency(double arrivalTime, long currentTime) {
    return (arrivalTime < 0) ? 0 : currentTime - arrivalTime;
  }

  public static double getHeadLatency(double[] features, long currentTime) {
    return getLatency(features[Features.HEAD_ARRIVAL_TIME.index()], currentTime);
  }

  public static double getAverageLatency(double[] features, long currentTime) {
    return getLatency(features[Features.AVERAGE_ARRIVAL_TIME.index()], currentTime);
  }

  public static boolean noArrivalTime(double arrivalTime) {
    return arrivalTime < 0;
  }

  public static boolean noArrivalTime(long arrivalTime) {
    return arrivalTime < 0;
  }

}
