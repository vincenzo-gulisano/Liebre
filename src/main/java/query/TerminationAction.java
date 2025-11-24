package query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * Action that terminates queries when all their sinks have finished. By
 * default, the TerminationAction assumes there's a single query running. Hence,
 * when all sinks are flushed, it terminates. if parameter singleQueryExecution
 * is set
 * to false, the TerminationAction keeps running until interrupted, checking
 * periodically for active queries and terminating them when all their sinks
 * have finished.
 */
public class TerminationAction implements Runnable {

  private final HashMap<Query, Set<String>> activeQueriesAndSinks;
  private final Object lock;
  private boolean singleQueryExecution;

  public TerminationAction(HashMap<Query, Set<String>> activeQueriesAndSinks, Object lock) {
    this(activeQueriesAndSinks, lock, true);
  }

  public TerminationAction(HashMap<Query, Set<String>> activeQueriesAndSinks, Object lock,
      boolean singleQueryExecution) {
    this.activeQueriesAndSinks = activeQueriesAndSinks;
    this.lock = lock;
    this.singleQueryExecution = singleQueryExecution;
  }

  public void setSingleQueryExecution(boolean singleQueryExecution) {
    this.singleQueryExecution = singleQueryExecution;
  }

  @Override
  public void run() {
    QueryTerminator.LOG.trace("Terminator started");
    boolean continueRunning = !Thread.currentThread().isInterrupted() && (!singleQueryExecution || !activeQueriesAndSinks.isEmpty());
    while (continueRunning) {
      synchronized (lock) {
        if (!activeQueriesAndSinks.isEmpty()) {
          Iterator<Map.Entry<Query, Set<String>>> it = activeQueriesAndSinks.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry<Query, Set<String>> activeQuery = it.next();
            QueryTerminator.LOG.trace("Active Sinks for Query {}: {}", activeQuery.getKey(), activeQuery.getValue());
            if (activeQuery.getValue().isEmpty()) {
              Query q = activeQuery.getKey();
              QueryTerminator.LOG.info("All sinks for Query {} have finished. Deactivating query.",q);
              it.remove();
              q.deActivate();
            }
          }
        }
      }
      try {
        Thread.sleep(QueryTerminator.TERMINATOR_POLL_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        QueryTerminator.LOG.trace("Terminator exiting");
        return;
      }
      continueRunning = !Thread.currentThread().isInterrupted() && (!singleQueryExecution || !activeQueriesAndSinks.isEmpty());
    }
  }
}