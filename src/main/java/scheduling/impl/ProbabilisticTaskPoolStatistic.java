/*
 * Copyright (C) 2017-2018
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

package scheduling.impl;

import common.util.StatisticFilename;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.priority.PriorityMetricFactory;

public class ProbabilisticTaskPoolStatistic extends ProbabilisticTaskPool {

  private final CSVPrinter csv;
  private final Logger LOGGER = LogManager.getLogger();

  public ProbabilisticTaskPoolStatistic(PriorityMetricFactory metricFactory,
      int priorityScalingFactor, long priorityUpdateIntervalNanos,
      String statisticsFolder) {
    super(metricFactory, priorityScalingFactor, priorityUpdateIntervalNanos);
    try {
      csv = new CSVPrinter(
          new FileWriter(StatisticFilename.INSTANCE.get(statisticsFolder, "taskPool", "prio")),
          CSVFormat.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected List<Double> updatePriorities(long threadId) {
    List<Double> priorities = super.updatePriorities(threadId);
    if (threadId == OWN_THREADID) {
      recordStatisticsHeader();
    }
    recordStatistics(priorities, threadId);
    return priorities;
  }

  private void recordStatisticsHeader() {
    try {
      csv.printRecord(tasks);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void recordStatistics(List<Double> probabilities, long threadId) {
    if (!isEnabled()) {
      LOGGER.debug("Ignoring append, TaskPool is disabled");
      return;
    }
    if (threadId % 4 == 0) {
      try {
        csv.printRecord(probabilities);
      } catch (IOException e) {
        LOGGER.error("Failed to record statistics for TaskPool", e);
      }
    }
  }

  @Override
  public void disable() {
    super.disable();
    try {
      csv.close();
    } catch (IOException e) {
      LOGGER.error("Failed to record statistics for TaskPool", e);
    }
  }
}
