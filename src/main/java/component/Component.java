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
import org.apache.commons.lang3.Validate;
import scheduling.toolkit.Feature;
import scheduling.toolkit.FeatureHelper;
import scheduling.toolkit.Task;

/**
 * Base interface for all stream components such as Sources, Sinks and Operators.
 *
 * @author palivosd
 * @see Active
 * @see Runnable
 * @see Named
 * @see ConnectionsNumber
 */
public interface Component extends Active, Runnable, Named, Task {

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

  double getRate();

  /**
   * Update the metrics  (e.g. cost and selectivity) based on the execution statistics of the
   * operator.
   * <br/>
   * <b>WARNING: The variables for the metrics are available only the execution happens with
   * {@link #runFor(int)}
   * !</b> <br/>
   * <b>WARNING: This is not thread safe! It should either be run from the operator thread or
   * from another thread while the operator is stopped. The results are visible to all threads.</b>
   */
  void updateMetrics();

  ComponentType getType();

  default double getAverageArrivalTime() {
    return FeatureHelper.NO_ARRIVAL_TIME;
  }

  default double getHeadArrivalTime() {
    return FeatureHelper.NO_ARRIVAL_TIME;
  }

  @Override
  default void updateFeatures(Feature[] features, double[] output) {
    Validate.notEmpty(features, "No features requested!");
    for (Feature feature : features) {
      output[feature.index()] = FeatureTranslator.get(feature, this);
    }
  }

  @Override
  default void refreshFeatures() {
    updateMetrics();
  }

  @Override
  default List<Component> getUpstream() {
    return Collections.emptyList();
  }

  @Override
  default List<Component> getDownstream() {
    return Collections.emptyList();
  }
}
