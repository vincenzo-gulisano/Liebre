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

package scheduling;

import static io.palyvos.haren.Features.*;

import component.Component;
import io.palyvos.haren.Feature;
import io.palyvos.haren.FeatureHelper;

public class FeatureTranslator {

  public static final double NO_ARRIVAL_TIME = FeatureHelper.NO_ARRIVAL_TIME;
  public static final long MAX_QUEUE_SIZE = FeatureHelper.MAX_QUEUE_SIZE;

  private FeatureTranslator() {

  }

  public static double get(Feature feature, Component component) {
    if (feature == COST) {
      return component.getCost();
    } else if (feature == SELECTIVITY) {
      return component.getSelectivity();
    } else if (feature == TOPOLOGICAL_ORDER) {
      return component.getTopologicalOrder();
    } else if (feature == COMPONENT_TYPE) {
      return translatedComponentType(component);
    } else if (feature == HEAD_ARRIVAL_TIME) {
      return component.getHeadArrivalTime();
    } else if (feature == AVERAGE_ARRIVAL_TIME) {
      return component.getAverageArrivalTime();
    } else if (feature == RATE) {
      return component.getRate();
    } else if (feature == USER_PRIORITY) {
      return component.getPriority();
    } else if (feature == INPUT_QUEUE_SIZE) {
      return component.getInputQueueSize();
    } else if (feature == OUTPUT_QUEUE_SIZE) {
      return component.getOutputQueueSize();
    } else {
      throw new IllegalStateException("Unknown feature: " + feature);
    }

  }

  static double translatedComponentType(Component component) {
    switch (component.getType()) {
      case SOURCE:
        return FeatureHelper.CTYPE_SOURCE;
      case SINK:
        return FeatureHelper.CTYPE_SINK;
      case OPERATOR:
        return FeatureHelper.CTYPE_OPERATOR;
      case OPERATOR2IN:
        return FeatureHelper.CTYPE_JOIN;
      case ROUTER:
        return FeatureHelper.CTYPE_ROUTER;
      case UNION:
        return FeatureHelper.CTYPE_UNION;
      default:
        throw new IllegalStateException("Unknown component type " + component.getType());
    }
  }
}
