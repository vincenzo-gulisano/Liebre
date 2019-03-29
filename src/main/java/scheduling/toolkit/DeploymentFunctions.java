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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class DeploymentFunctions {

  private DeploymentFunctions() {

  }

  public static DeploymentFunction roundRobinQuery() {
    return new RoundRobinDeployment();
  }

  public static DeploymentFunction adaptiveLatency() {
    return new LatencyAdaptiveDeployment();
  }

  public static DeploymentFunction randomOperator() {
    return new RandomOperatorDeployment();
  }

  private static class RandomOperatorDeployment extends AbstractDeploymentFunction {

    private final Random random = new Random();

    public RandomOperatorDeployment() {
      super("RANDOM_OPERATOR", Feature.USER_PRIORITY);
    }

    @Override
    public void init(List<Task> tasks, double[][] features) {
      super.init(tasks, features);
    }

    @Override
    public List<List<Task>> getDeployment(int nThreads) {
      Validate.isTrue(nThreads > 0, "nThreads > 0");
      List<List<Task>> assignments = new ArrayList<>();
      for (int i = 0; i < nThreads; i++) {
        assignments.add(new ArrayList<>());
      }
      for (Task task : tasks) {
        assignments.get(random.nextInt(nThreads)).add(task);
      }
      return assignments;
    }


  }

  private static class RoundRobinDeployment extends AbstractDeploymentFunction {

    private QueryResolver queries;

    protected RoundRobinDeployment() {
      super("ROUND_ROBIN_QUERY", new Feature[0]);
    }

    @Override
    public void init(List<Task> tasks, double[][] features) {
      super.init(tasks, features);
      queries = new QueryResolver(tasks);
    }

    @Override
    public List<List<Task>> getDeployment(int nThreads) {
      Validate.isTrue(nThreads > 0, "nThreads > 0");
      List<List<Task>> assignments = new ArrayList<>();
      for (int i = 0; i < nThreads; i++) {
        assignments.add(new ArrayList<>());
      }
      int assignmentIndex = 0;
      for (List<Task> query : queries.getQueries()) {
        assignments.get(assignmentIndex % assignments.size()).addAll(query);
        assignmentIndex++;
      }
      return assignments;
    }
  }

  private static class LatencyAdaptiveDeployment extends AbstractDeploymentFunction {

    // Relative difference (percentage) in latency that
    // triggers increase in thread number
    private static final long RELATIVE_DIFF_LIMIT = 10;
    private static final long RELATIVE_DIFF_DROP_LIMIT = 20 * RELATIVE_DIFF_LIMIT;
    private static final long UPDATE_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final double alpha = 0.3;
    private static final Logger LOG = LogManager.getLogger();
    private final DeploymentFunction roundRobinFunction;
    private List<Task> sinks = new ArrayList<>();
    private long checkpointLatency = -1;
    private long runningAverageLatency = -1;
    private int usedThreads = 1;
    private long lastUpdateTime = -1;

    protected LatencyAdaptiveDeployment() {
      super("ADAPTIVE_LATENCY", Feature.AVERAGE_ARRIVAL_TIME, Feature.COMPONENT_TYPE);
      roundRobinFunction = new RoundRobinDeployment();
    }

    @Override
    public void init(List<Task> tasks, double[][] features) {
      super.init(tasks, features);
      roundRobinFunction.init(tasks, features);
    }

    @Override
    public List<List<Task>> getDeployment(int nThreads) {
      initSinks();
      long sinkLatency = getSinkLatency();
      updateAverageLatency(sinkLatency);
      updateAndAdapt(nThreads);
      return roundRobinFunction.getDeployment(usedThreads);
    }

    private void initSinks() {
      if (sinks.size() > 0) {
        return;
      }
      for (Task task : tasks) {
        if (Feature.COMPONENT_TYPE.get(task, features) == FeatureHelper.CTYPE_SINK) {
          sinks.add(task);
        }
      }
    }

    private void updateAverageLatency(long latency) {
      runningAverageLatency = runningAverageLatency < 0 ? latency :
          Math.round((alpha * latency) + ((1 - alpha) * runningAverageLatency));
    }

    private void updateAndAdapt(int nThreads) {
      long currentTime = System.currentTimeMillis();
      if ((lastUpdateTime == 0 ||
          currentTime - lastUpdateTime > UPDATE_PERIOD_MILLIS) && runningAverageLatency > 0) {
        LOG.debug("Updating latency from {} to {}", checkpointLatency, runningAverageLatency);
        adapt(nThreads);
        checkpointLatency = runningAverageLatency;
        lastUpdateTime = currentTime;
      }
    }

    private void adapt(int nThreads) {
      if (runningAverageLatency < 0 || checkpointLatency < 0) {
        return;
      }
      double latencyRelativeDiff =
          100 * (runningAverageLatency - checkpointLatency) / (double) checkpointLatency;
      if (latencyRelativeDiff > RELATIVE_DIFF_LIMIT) {
        usedThreads = Math.min(usedThreads + 1, nThreads);
        LOG.info("Latency increased by {}% ({} ms). Increasing threads to {}",
            Math.round(latencyRelativeDiff), runningAverageLatency - checkpointLatency,
            usedThreads);
      } else if (latencyRelativeDiff < Math.negateExact(RELATIVE_DIFF_DROP_LIMIT)) {
        usedThreads = Math.max(usedThreads - 1, 1);
        LOG.info("Latency decreased by {}% ({} ms). decreasing threads to {}",
            Math.round(latencyRelativeDiff), runningAverageLatency - checkpointLatency,
            usedThreads);
      }
    }

    private long getSinkLatency() {
      long total = 0;
      long count = 0;
      for (Task sink : sinks) {
        long headArrivalTime = Math.round(Feature.AVERAGE_ARRIVAL_TIME.get(sink, features));
        if (FeatureHelper.noArrivalTime(headArrivalTime)) {
          continue;
        }
        total += System.currentTimeMillis() - headArrivalTime;
        count += 1;
      }
      return count > 0 ? total / count : -1;
    }
  }

}
