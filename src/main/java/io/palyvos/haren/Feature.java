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

/**
 * An abstraction of anything that characterizes a {@link Task}'s state.
 */
public interface Feature {

  /**
   * @return The unique, numerical index of this feature.
   */
  int index();

  /**
   * Check if this is a constant or variable feature.
   *
   * @return {@code true} if the feature is constant.
   */
  boolean isConstant();

  /**
   * Get the value of this feature.
   *
   * @param task The task that we want to retrieve the feature for.
   * @param features The complete feature matrix of the SPE.
   * @return The value of this feature.
   */
  double get(Task task, double[][] features);

  /**
   * Get the dependencies of this feature to other features and tasks. It is possible for a feature
   * to depend on itself (for example, when a feature changes, the same feature needs to be
   * updated for upstream or downstream tasks).
   *
   * @return An array of {@link FeatureDependency} objects.
   */
  FeatureDependency[] dependencies();
}
