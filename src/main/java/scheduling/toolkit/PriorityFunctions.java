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

import static scheduling.toolkit.FeatureHelper.CTYPE_SOURCE;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;

public class PriorityFunctions {

  private static final SinglePriorityFunction TUPLE_PROCESSING_TIME = new CachingPriorityFunction(
      "TUPLE_PROCESSING_TIME", Feature.COST) {
    @Override
    public double applyWithCachingSupport(Task task, double[][] features,
        List<Task> tasks) {
      double totalProcessingTime = Feature.COST.get(task, features);
      for (Task downstream : task.getDownstream()) {
        totalProcessingTime += apply(downstream, features, tasks);
      }
      return totalProcessingTime;
    }

    @Override
    public boolean reverseOrder() {
      return true;
    }
  };

  private static final SinglePriorityFunction GLOBAL_SELECTIVITY = new CachingPriorityFunction(
      "GLOBAL_SELECTIVITY", Feature.SELECTIVITY) {
    @Override
    public double applyWithCachingSupport(Task task, double[][] features,
        List<Task> tasks) {
      double globalSelectivity = Feature.SELECTIVITY.get(task, features);
      for (Task downstream : task.getDownstream()) {
        globalSelectivity *= apply(downstream, features, tasks);
      }
      return globalSelectivity;
    }
  };

  private static final SinglePriorityFunction HEAD_ARRIVAL_TIME = new AbstractPriorityFunction(
      "HEAD_ARRIVAL_TIME", Feature.HEAD_ARRIVAL_TIME) {
    @Override
    public double apply(Task task, double[][] features,
        List<Task> tasks) {
      return Feature.HEAD_ARRIVAL_TIME.get(task, features);
    }

    @Override
    public boolean reverseOrder() {
      return true;
    }
  };

  private static final SinglePriorityFunction AVERAGE_ARRIVAL_TIME = new AbstractPriorityFunction(
      "AVERAGE_ARRIVAL_TIME", Feature.AVERAGE_ARRIVAL_TIME) {
    @Override
    public double apply(Task task, double[][] features,
        List<Task> tasks) {
      return Feature.AVERAGE_ARRIVAL_TIME.get(task, features);
    }

    @Override
    public boolean reverseOrder() {
      return true;
    }

  };

  private static final SinglePriorityFunction GLOBAL_AVERAGE_COST =
      new CachingPriorityFunction("GLOBAL_AVERAGE_COST", Feature.COST, Feature.SELECTIVITY) {

        @Override
        public double applyWithCachingSupport(Task task, double[][] features,
            List<Task> tasks) {
          double globalAverageCost = Feature.COST.get(task, features);
          double selectivity = Feature.SELECTIVITY.get(task, features);
          for (Task downstream : task.getDownstream()) {
            globalAverageCost += selectivity * apply(downstream, features, tasks);
          }
          return globalAverageCost;
        }

        @Override
        public boolean reverseOrder() {
          return true;
        }
      };
  private static final SinglePriorityFunction GLOBAL_RATE =
      new AbstractPriorityFunction("GLOBAL_RATE", GLOBAL_SELECTIVITY, GLOBAL_AVERAGE_COST) {
        @Override
        public double apply(Task task, double[][] features,
            List<Task> tasks) {
          return GLOBAL_SELECTIVITY.apply(task, features, tasks) / GLOBAL_AVERAGE_COST
              .apply(task, features, tasks);
        }

      };
  private static final SinglePriorityFunction GLOBAL_NORMALIZED_RATE =
      new AbstractPriorityFunction("GLOBAL_NORMALIZED_RATE", GLOBAL_SELECTIVITY,
          GLOBAL_AVERAGE_COST, TUPLE_PROCESSING_TIME) {
        @Override
        public double apply(Task task, double[][] features,
            List<Task> tasks) {
          return GLOBAL_SELECTIVITY.apply(task, features, tasks) / (GLOBAL_AVERAGE_COST.apply(task,
              features, tasks)
              * TUPLE_PROCESSING_TIME.apply(task, features, tasks));
        }
      };

  private static final SinglePriorityFunction USER_PRIORITY =
      new AbstractPriorityFunction("USER_PRIORITY", Feature.USER_PRIORITY) {
        @Override
        public double apply(Task task, double[][] features,
            List<Task> tasks) {
          return Feature.USER_PRIORITY.get(task, features);
        }
      };

  private PriorityFunctions() {

  }

  public static SinglePriorityFunction averageArrivalTime() {
    return AVERAGE_ARRIVAL_TIME;
  }

  public static SinglePriorityFunction headArrivalTime() {
    return HEAD_ARRIVAL_TIME;
  }

  public static SinglePriorityFunction globalNormalizedRate() {
    return GLOBAL_NORMALIZED_RATE;
  }

  public static SinglePriorityFunction globalRate() {
    return GLOBAL_RATE;
  }

  public static SinglePriorityFunction tupleProcessingTime() {
    return TUPLE_PROCESSING_TIME;
  }

  public static SinglePriorityFunction globalAverageCost() {
    return GLOBAL_AVERAGE_COST;
  }

  public static SinglePriorityFunction userPriority() {
    return USER_PRIORITY;
  }

  public static SinglePriorityFunction chain() {
    return new ChainPriorityFunction();
  }

  public static SinglePriorityFunction reciprocalFunction(SinglePriorityFunction function) {
    return new ReciprocalPriorityFunction(function);
  }

  private static class ChainPriorityFunction extends AbstractPriorityFunction {

    private double[] sdop;
    private int nTasks;
    private boolean warm = false;

    public ChainPriorityFunction() {
      super("CHAIN", GLOBAL_AVERAGE_COST, GLOBAL_SELECTIVITY);
    }

    @Override
    public double apply(Task task, double[][] features, List<Task> tasks) {
      if (!warm) {
        calculateEnvelopes(tasks, features);
      }
      return sdop[task.getIndex()];
    }

    private void calculateEnvelopes(List<Task> tasks, double[][] features) {
      List<Task> sources = getSources(tasks, features);
      for (Task source : sources) {
        populateCache(source, features, tasks);
      }
      warm = true;
    }

    private void populateCache(Task task, double[][] features, List<Task> tasks) {
      List<Task> lowerEnvelopeCandidates = new ArrayList<>();
      Task downstream = getDownstream(task);
      Task selected = null;
      double maxDerivative = -1;
      while (true) {
        if (downstream == null) {
          break;
        }
        double derivative = getDerivative(task, downstream, features, tasks);
        if (derivative > maxDerivative) {
          maxDerivative = derivative;
          selected = downstream;
        }
        lowerEnvelopeCandidates.add(downstream);
        downstream = getDownstream(downstream);
      }
      for (Task candidate : lowerEnvelopeCandidates) {
        sdop[candidate.getIndex()] = maxDerivative;
        if (candidate.getIndex() == selected.getIndex()) {
          break;
        }
      }
      if (selected != null) {
        populateCache(selected, features, tasks);
      }
    }

    private Task getDownstream(Task task) {
      List<? extends Task> downstream = task.getDownstream();
      Validate.isTrue(downstream.size() <= 1, "This implementation works only for chains!");
      return downstream.size() == 0 ? null : downstream.get(0);
    }

    private double getDerivative(Task left, Task right, double[][] features, List<Task> tasks) {
      double selectivityChange =
          GLOBAL_SELECTIVITY.apply(left, features, tasks) / GLOBAL_SELECTIVITY.apply(right,
              features, tasks);
      double costChange =
          GLOBAL_AVERAGE_COST.apply(left, features, tasks) - GLOBAL_AVERAGE_COST.apply(right,
              features, tasks);
      double derivative = (1 - selectivityChange) / costChange;
      return Double.isNaN(derivative) ? 0 : derivative;
    }

    private List<Task> getSources(List<Task> tasks, double[][] features) {
      final List<Task> result = new ArrayList<>();
      for (Task task : tasks) {
        if (Feature.COMPONENT_TYPE.get(task, features) == CTYPE_SOURCE) {
          result.add(task);
        }
      }
      return result;
    }

    @Override
    public SinglePriorityFunction enableCaching(int nTasks) {
      super.enableCaching(nTasks);
      sdop = new double[nTasks];
      return this;
    }

    @Override
    public void clearCache() {
      super.clearCache();
      for (int i = 0; i < sdop.length; i++) {
        sdop[i] = 0;
      }
      warm = false;
    }
  }

  private static class ReciprocalPriorityFunction implements SinglePriorityFunction {

    private final SinglePriorityFunction original;

    private ReciprocalPriorityFunction(SinglePriorityFunction original) {
      this.original = original;
    }

    @Override
    public double apply(Task task, double[][] features,
        List<Task> tasks) {
      return 1 / original.apply(task, features, tasks);
    }

    @Override
    public Feature[] requiredFeatures() {
      return original.requiredFeatures();
    }

    @Override
    public SinglePriorityFunction enableCaching(int nTasks) {
      return original.enableCaching(nTasks);
    }

    @Override
    public void clearCache() {
      original.clearCache();
    }

    @Override
    public String name() {
      return original.name() + "_reciprocal";
    }
  }

}
