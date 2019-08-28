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

import java.util.Comparator;
import org.apache.commons.lang3.Validate;

public class MultiPriorityComparator implements Comparator<Task> {

  private final double[][] priorities;
  private final boolean[] reverseOrder;

  public MultiPriorityComparator(MultiPriorityFunction function, double[][] priorities) {
    Validate.isTrue(function.dimensions() == priorities[0].length);
    this.priorities = priorities;
    this.reverseOrder = new boolean[function.dimensions()];
    for (int i = 0; i < function.dimensions(); i++) {
      reverseOrder[i] = function.reverseOrder(i);
    }
  }

  @Override
  public int compare(Task task1, Task task2) {
    double[] p1 = priorities[task1.getIndex()];
    double[] p2 = priorities[task2.getIndex()];
    // Compare the priorities of all dimensions
    for (int k = 0; k < p1.length; k++) {
      int dimensionComparison = Double.compare(p1[k], p2[k]);
      if (dimensionComparison != 0) {
        // Reverse order means from LOW -> HIGH (so same as Double.compare)
        // Normal order for priorities is HIGH -> LOW
        return reverseOrder[k] ? dimensionComparison : -dimensionComparison;
      }
    }
    // If all dimensions had equal priorities, tasks are equal
    return 0;
  }
}
