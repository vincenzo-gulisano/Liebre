package scheduling.impl;

import common.component.Component;
import common.util.AliasMethod;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import scheduling.TaskPool;
import scheduling.priority.PriorityMetric;
import scheduling.priority.PriorityMetricFactory;

public class ProbabilisticTaskPool implements TaskPool<Component> {

  protected final List<Component> tasks = new ArrayList<>();
  protected final List<Component> passiveTasks = new ArrayList<>();
  private final PriorityMetricFactory metricFactory;
  private final AtomicReference<Turn> turns;
  private final int priorityScalingFactor;

  private AtomicReference<AliasMethod> sampler = new AtomicReference<AliasMethod>(null);
  private volatile PriorityMetric metric;
  private volatile int nThreads;
  private AtomicReferenceArray<Boolean> available;
  private volatile boolean enabled;

  public ProbabilisticTaskPool(PriorityMetricFactory metricFactory, int priorityScalingFactor,
      long priorityUpdateIntervalNanos) {
    this.metricFactory = metricFactory;
    this.priorityScalingFactor = priorityScalingFactor;
    this.turns = new AtomicReference<Turn>(new Turn(0, System.nanoTime(),
        priorityUpdateIntervalNanos));
  }

  @Override
  public void register(Component task) {
    if (isEnabled()) {
      throw new IllegalStateException("Cannot add tasks in an enabled TaskPool!");
    }
    tasks.add(task);
  }

  @Override
  public void registerPassive(Component task) {
    if (isEnabled()) {
      throw new IllegalStateException("Cannot add tasks in an enabled TaskPool!");
    }
    tasks.add(task);
    passiveTasks.add(task);
  }

  @Override
  public Component getNext(int threadId) {

    Turn turn = turns.get();
    if (turn.isTime(threadId)) {
      updatePriorities(threadId);
      turns.set(turn.next(nThreads));
    }
    AliasMethod alias = sampler.get();
    while (true) {
      int k = alias.next();
      if (available.compareAndSet(k, true, false)) {
        return tasks.get(k);
      }
    }
  }

  @Override
  public void put(Component task, int threadId) {
    available.set(task.getIndex(), true);
  }

  protected List<Double> updatePriorities(long threadId) {
    List<Double> probabilities = metric.getPriorities(priorityScalingFactor);
    sampler.set(new AliasMethod(probabilities));
    return probabilities;
  }

  @Override
  public void setThreadsNumber(int activeThreads) {
    if (isEnabled()) {
      throw new IllegalStateException("Cannot set threads number when TaskPool is enabled");
    }
    this.nThreads = activeThreads;
  }

  @Override
  public void enable() {

    if (nThreads == 0) {
      throw new IllegalStateException("Thread number not set!");
    }
    // Initialize locks and operator index
    available = new AtomicReferenceArray<>(tasks.size());
    // Sort tasks according to their indexes
    tasks.sort(
        (Component t1, Component t2) -> Integer.compare(t1.getIndex(), t2.getIndex()));
    metric = metricFactory.newInstance(tasks, passiveTasks, nThreadsTotal());
    for (Component task : tasks) {
      boolean isActive = !passiveTasks.contains(task);
      // Only the active tasks can be picked for execution
      // by the task pool
      available.set(task.getIndex(), isActive);
      task.setPriorityMetric(metric);
      task.enable();
    }
    // Initialize priorities
    updatePriorities(1);
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return this.enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    for (Component t : tasks) {
      t.disable();
    }
  }

  private int nThreadsTotal() {
    return nThreads + passiveTasks.size();
  }

  private static class Turn {

    private final long ts;
    private final long threadId;
    private final long turnPeriodNanos;


    public Turn(long threadId, long ts, long turnPeriodNanos) {
      this.threadId = threadId;
      this.ts = ts;
      this.turnPeriodNanos = turnPeriodNanos;
    }

    public Turn next(int nThreads) {
      return new Turn((threadId + 1) % nThreads, System.nanoTime(), turnPeriodNanos);
    }

    public Turn skip(int nThreads) {
      return new Turn((threadId + 1) % nThreads, ts, turnPeriodNanos);
    }

    public boolean isTime(long threadId) {
      return this.threadId == threadId && (System.nanoTime() >= ts + turnPeriodNanos);
    }
  }

}
