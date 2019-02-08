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

package component;

import scheduling.toolkit.Feature;
import scheduling.toolkit.FeatureHelper;

class FeatureTranslator {

  private FeatureTranslator() {

  }

  static double get(Feature feature, Component component) {
    switch (feature) {
      case COST:
        return component.getCost();
      case SELECTIVITY:
        return component.getSelectivity();
      case TOPOLOGICAL_ORDER:
        return component.getTopologicalOrder();
      case COMPONENT_TYPE:
        return translatedComponentType(component);
      case HEAD_ARRIVAL_TIME:
        return component.getHeadArrivalTime();
      case AVERAGE_ARRIVAL_TIME:
        return component.getAverageArrivalTime();
      default:
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
