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

import static io.palyvos.haren.FeatureHelper.CTYPE_SOURCE;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ChainIntraThreadSchedulingFunction extends AbstractIntraThreadSchedulingFunction {

  public static final int NOT_INITIALIZED = -1;
  private static final Logger LOG = LogManager.getLogger();
  private static final SingleIntraThreadSchedulingFunction TOTAL_SELECTIVITY = new CachingIntraThreadSchedulingFunction(
      "TOTAL_SELECTIVITY", Feature.SELECTIVITY, Feature.COMPONENT_TYPE) {
    @Override
    protected double applyWithCachingSupport(Task task, double[][] features) {
      Task upstream = getUpstream(task);
      double selectivity = getSelectivity(task, features);
      if (upstream == null) {
        return selectivity;
      } else {
        return selectivity * apply(upstream, features);
      }
    }


  };
  private static final SingleIntraThreadSchedulingFunction TIME = new CachingIntraThreadSchedulingFunction(
      "TOTAL_TIME", Feature.COST, Feature.SELECTIVITY) {

    @Override
    protected double applyWithCachingSupport(Task task, double[][] features) {
      Task upstream = getUpstream(task);
      if (upstream == null) {
        return Feature.COST.get(task, features);
      } else {
        return Feature.COST.get(task, features) * TOTAL_SELECTIVITY.apply(upstream, features)
            + apply(upstream, features);
      }
    }
  };
  public static final double SINK_SELECTIVITY = 0;
  private double[] sdop;
  private boolean warm = false;

  public ChainIntraThreadSchedulingFunction() {
    super("CHAIN", TOTAL_SELECTIVITY, TIME);
  }

  private static Task getDownstream(Task task) {
    List<? extends Task> downstream = task.getDownstream();
    Validate.isTrue(downstream.size() <= 1, "This implementation works only for chains!");
    return downstream.size() == 0 ? null : downstream.get(0);
  }

  private static Task getUpstream(Task task) {
    List<? extends Task> upstream = task.getUpstream();
    Validate.isTrue(upstream.size() <= 1, "This implementation works only for chains!");
    return upstream.size() == 0 ? null : upstream.get(0);
  }

  private static double getSelectivity(Task task, double[][] features) {
    boolean isSink = Feature.COMPONENT_TYPE.get(task, features) == FeatureHelper.CTYPE_SINK;
    return isSink ? SINK_SELECTIVITY : Feature.SELECTIVITY.get(task, features);
  }

  @Override
  public double apply(Task task, double[][] features) {
    Validate.isTrue(cachingEnabled(), "This function cannot work without caching!");
    if (sdop[task.getIndex()] < 0) {
      Task source = getSource(task, features);
      calculateLowerEnvelope(source, features);
    }
    return sdop[task.getIndex()];
  }

  private void calculateLowerEnvelope(Task task, double[][] features) {
    if (task == null) {
      return;
    }
    List<Task> lowerEnvelopeCandidates = new ArrayList<>();
    lowerEnvelopeCandidates.add(task);
    Task downstream = task;
    Task selected = null;
    double maxDerivative = NOT_INITIALIZED;
    while ((downstream = getDownstream(downstream)) != null) {
      double derivative = getDerivative(task, downstream, features);
      if (derivative > maxDerivative) {
        maxDerivative = derivative;
        selected = downstream;
      }
      lowerEnvelopeCandidates.add(downstream);
    }
    // Create the envelope
    if (selected != null) {
      if (LOG.getLevel() == Level.TRACE) {
        printDetails(features, lowerEnvelopeCandidates, selected, maxDerivative);
      }
      for (Task candidate : lowerEnvelopeCandidates) {
        sdop[candidate.getIndex()] = maxDerivative;
        if (candidate.getIndex() == selected.getIndex()) {
          break;
        }
      }
      calculateLowerEnvelope(selected, features);
    }
  }

  private void printDetails(double[][] features, List<Task> lowerEnvelopeCandidates,
      Task selected,
      double maxDerivative) {
    System.out.println("\n---------------------");
    System.out.println(lowerEnvelopeCandidates);
    System.out.format("cost = [%s]%n",
        lowerEnvelopeCandidates.stream().map(t -> Double.toString(
            Feature.COST.get(t, features))).collect(Collectors.joining(",")));
    System.out.format("selectivity = [%s]%n",
        lowerEnvelopeCandidates.stream().map(t -> Double.toString(
            getSelectivity(t, features))).collect(Collectors.joining(",")));
    System.out.println("> " + selected);
    System.out.println("> " + maxDerivative);
  }

  private double getDerivative(Task left, Task right, double[][] features) {
    double selectivityChange =
        TOTAL_SELECTIVITY.apply(right, features) - TOTAL_SELECTIVITY.apply(left,
            features);
    double timeChange = TIME.apply(right, features) - TIME.apply(left, features);
    return -selectivityChange / timeChange;
  }

  private Task getSource(Task task, double[][] features) {
    if (Feature.COMPONENT_TYPE.get(task, features) == CTYPE_SOURCE) {
      return task;
    }
    return getSource(getUpstream(task), features);
  }

  @Override
  public SingleIntraThreadSchedulingFunction enableCaching(int nTasks) {
    super.enableCaching(nTasks);
    sdop = new double[nTasks];
    for (int i = 0; i < nTasks; i++) {
      sdop[i] = NOT_INITIALIZED;
    }
    return this;
  }

  @Override
  public void clearCache() {
    super.clearCache();
    for (int i = 0; i < sdop.length; i++) {
      sdop[i] = NOT_INITIALIZED;
    }
  }
}
