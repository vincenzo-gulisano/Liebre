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
  public void enable() {
    recordStatisticsHeader();
    super.enable();
  }

  @Override
  protected List<Double> updatePriorities(long threadId) {
    List<Double> priorities = super.updatePriorities(threadId);
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
