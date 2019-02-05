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

public class PriorityFunctions {

  private static final PriorityFunction TUPLE_PROCESSING_TIME = new PriorityFunction() {
    @Override
    public double apply(Task task) {
      double totalProcessingTime = task.getFeatures()[Features.F_COST];
      for (Task downstream : task.getDownstream()) {
        totalProcessingTime += apply(downstream);
      }
      return totalProcessingTime;
    }
  };

  private static final PriorityFunction GLOBAL_SELECTIVITY = new PriorityFunction() {
    @Override
    public double apply(Task task) {
      double globalSelectivity = task.getFeatures()[Features.F_SELECTIVITY];
      for (Task downstream : task.getDownstream()) {
        globalSelectivity *= apply(downstream);
      }
      return globalSelectivity;
    }
  };

  private static final PriorityFunction GLOBAL_AVERAGE_COST = new PriorityFunction() {
    @Override
    public double apply(Task task) {
      double globalAverageCost = task.getFeatures()[Features.F_COST];
      double selectivity = task.getFeatures()[Features.F_SELECTIVITY];
      for (Task downstream : task.getDownstream()) {
        globalAverageCost += selectivity * apply(downstream);
      }
      return globalAverageCost;
    }
  };

  private static final PriorityFunction GLOBAL_RATE = task ->
      GLOBAL_SELECTIVITY.apply(task) / GLOBAL_AVERAGE_COST.apply(task);

  private static final PriorityFunction GLOBAL_NORMALIZED_RATE =
      task -> GLOBAL_SELECTIVITY.apply(task) / (GLOBAL_AVERAGE_COST.apply(task)
          * TUPLE_PROCESSING_TIME
          .apply(task));

  private static final PriorityFunction AVERAGE_LATENCY =
      task -> Features.getAverageLatency(task.getFeatures(), System.currentTimeMillis());

  private PriorityFunctions() {

  }

  public static PriorityFunction averageLatency() {
    //FIXME: Not a correct way to compute latency, use decreasing arrival time instead
    System.err.println(">>>>>> [WARNING] Average lantecy is an approximation. Should use "
        + "decreasing arrival time instead!!!!!!");
    return AVERAGE_LATENCY;
  }

  public static PriorityFunction globalNormalizedRate() {
    return GLOBAL_NORMALIZED_RATE;
  }

  public static PriorityFunction globalRate() {
    return GLOBAL_RATE;
  }

  public static PriorityFunction tupleProcessingTime() {
    return TUPLE_PROCESSING_TIME;
  }

  public static PriorityFunction globalAverageCost() {
    return GLOBAL_AVERAGE_COST;
  }

  public static PriorityFunction reciprocalFunction(PriorityFunction function) {
    return new ReciprocalPriorityFunction(function);
  }

  private static class ReciprocalPriorityFunction implements PriorityFunction {

    private final PriorityFunction original;

    private ReciprocalPriorityFunction(PriorityFunction original) {
      this.original = original;
    }

    @Override
    public double apply(Task task) {
      return 1 / original.apply(task);
    }
  }

}
