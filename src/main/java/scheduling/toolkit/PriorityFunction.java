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

public interface PriorityFunction {

  double apply(Task task, double[][] features);

  Feature[] features();

  // True if lower values of priority imply higher priority
  default boolean reverseOrder() {
   return false;
  }

  default PriorityFunction reciprocal() {
    return PriorityFunctions.reciprocalFunction(this);
  }

  default double apply(Task task, double[][] features, double[][] cache) {
    System.err.println("Caching not implemented for this function");
    return apply(task, features);
  }

  default double[][] newCache(int nTasks) {
    System.err.println("Caching not implemented for this function");
    return new double[0][0];
  }

}
