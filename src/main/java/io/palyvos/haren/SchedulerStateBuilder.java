package io.palyvos.haren;

import io.palyvos.haren.function.InterThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SchedulerStateBuilder {

  private int nTasks;
  private VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction;
  private InterThreadSchedulingFunction interThreadSchedulingFunction;
  private boolean priorityCaching;
  private String statisticsFolder;
  private int nThreads;
  private long schedulingPeriod;
  private int batchSize;

  public SchedulerStateBuilder setTaskNumber(int nTasks) {
    this.nTasks = nTasks;
    return this;
  }

  public SchedulerStateBuilder setIntraThreadSchedulingFunction(
      VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction) {
    this.intraThreadSchedulingFunction = intraThreadSchedulingFunction;
    return this;
  }

  public SchedulerStateBuilder setInterThreadSchedulingFunction(
      InterThreadSchedulingFunction interThreadSchedulingFunction) {
    this.interThreadSchedulingFunction = interThreadSchedulingFunction;
    return this;
  }

  public SchedulerStateBuilder setPriorityCaching(boolean priorityCaching) {
    this.priorityCaching = priorityCaching;
    return this;
  }

  public SchedulerStateBuilder setStatisticsFolder(String statisticsFolder) {
    this.statisticsFolder = statisticsFolder;
    return this;
  }

  public SchedulerStateBuilder setThreadNumber(int nThreads) {
    this.nThreads = nThreads;
    return this;
  }

  public SchedulerStateBuilder setSchedulingPeriod(long schedulingPeriod) {
    this.schedulingPeriod = schedulingPeriod;
    return this;
  }

  public SchedulerStateBuilder setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public SchedulerState createSchedulerState() {
    return new SchedulerState(nTasks, intraThreadSchedulingFunction, interThreadSchedulingFunction,
        priorityCaching, statisticsFolder, nThreads, schedulingPeriod, batchSize);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
        .append("nTasks", nTasks)
        .append("nThreads", nThreads)
        .append("priorityCaching", priorityCaching)
        .append("intraThreadSchedulingFunction", intraThreadSchedulingFunction)
        .append("interThreadSchedulingFunction", interThreadSchedulingFunction)
        .append("schedulingPeriod", schedulingPeriod)
        .append("batchSize", batchSize)
        .append("statisticsFolder", statisticsFolder)
        .toString();
  }
}