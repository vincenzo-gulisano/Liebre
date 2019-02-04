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

import common.Active;
import common.Named;
import java.util.Collections;
import java.util.List;
import scheduling.toolkit.ExecutableComponent;
import scheduling.toolkit.Features;

/**
 * Base interface for all stream components such as Sources, Sinks and Operators.
 *
 * @author palivosd
 * @see Active
 * @see Runnable
 * @see Named
 * @see ConnectionsNumber
 */
public interface Component extends Active, Runnable, Named, ExecutableComponent {

  /**
   * The input {@link ConnectionsNumber} of this component. Used to enforce invariants during
   * query construction.
   *
   * @return The input {@link ConnectionsNumber} of this component.
   */
  ConnectionsNumber inputsNumber();

  /**
   * The output {@link ConnectionsNumber} of this component. Used to enforce invariants during
   * query construction.
   *
   * @return The output {@link ConnectionsNumber} of this component.
   */
  ConnectionsNumber outputsNumber();

  int getTopologicalOrder();

  double getCost();

  double getSelectivity();

  void updateMetrics();

  ComponentType getType();

  @Override
  default double[] getFeatures() {
    double[] features = Features.create();
    updateMetrics();
    features[Features.F_TOPOLOGICAL_ORDER] = getTopologicalOrder();
    features[Features.F_COST] = getCost();
    features[Features.F_SELECTIVITY] = getSelectivity();
    features[Features.F_HEAD_ARRIVAL_TIME] = -1; // Negative value means no data
    if (this instanceof StreamConsumer) {
      features[Features.F_HEAD_ARRIVAL_TIME] = ((StreamConsumer) this).getHeadArrivalTime();
    }
    int translatedType = -1;
    switch (getType()) {
      case SOURCE:
        translatedType = Features.CTYPE_SOURCE;
        break;
      case SINK:
        translatedType = Features.CTYPE_SINK;
        break;
      case OPERATOR:
        translatedType = Features.CTYPE_OPERATOR;
        break;
      case OPERATOR2IN:
        translatedType = Features.CTYPE_JOIN;
        break;
      case ROUTER:
        translatedType = Features.CTYPE_ROUTER;
        break;
      case UNION:
        translatedType = Features.CTYPE_UNION;
        break;
      default:
        throw new IllegalStateException("Unknown component type " + getType());
    }
    features[Features.F_COMPONENT_TYPE] = translatedType;
    return features;
  }

  @Override
  default List<ExecutableComponent> getUpstream() {
    return Collections.emptyList();
  }

  @Override
  default List<ExecutableComponent> getDownstream() {
    return Collections.emptyList();
  }
}
