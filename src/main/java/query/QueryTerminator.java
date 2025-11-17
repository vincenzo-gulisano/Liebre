package query;

import component.sink.Sink;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import common.Named;

/**
 * Helper class that manages the termination of queries when all their sinks
 * have finished. By default, the QueryTerminator assumes there's a single query
 * running. Hence, when all sinks are flushed, it terminates. if parameter
 * singleQueryExecution is set to false, the QueryTerminator keeps running until
 * interrupted, checking periodically for active queries and terminating them
 * when all their sinks have finished.
 */
class QueryTerminator {

  static final Logger LOG = LogManager.getLogger(QueryTerminator.class);
  static final int TERMINATOR_POLL_INTERVAL_MILLIS = 50;

  private final HashMap<Query, Set<String>> activeQueriesAndSinks = new HashMap<>();
  private Thread terminatorThread;

  private final TerminationAction terminationAction;

  private boolean hasBeenActivated = false;

  /** Protects all accesses to activeQueriesAndSinks */
  private final Object lock = new Object();

  QueryTerminator() {
    this(true);
  }

  QueryTerminator(boolean singleQueryExecution) {
    this.terminationAction = new TerminationAction(activeQueriesAndSinks, lock,
        singleQueryExecution);
    
  }

  public boolean hasBeenActivated() {
    return hasBeenActivated;
  }

  public void activate() {
    synchronized (lock) {
      if (hasBeenActivated) {
        LOG.warn("QueryTerminator has already been activated.");
        return;
      }
      this.hasBeenActivated = true;
      terminatorThread = new Thread(terminationAction, "QueryTerminatorThread");
      terminatorThread.start();
    }
  }

  public void setSingleQueryExecution(boolean singleQueryExecution) {
    this.terminationAction.setSingleQueryExecution(singleQueryExecution);
  }

  public void registerQuery(Query query) {
    if (query == null) {
      LOG.error("Cannot register null query");
      throw new IllegalArgumentException("Cannot register null query");
    }
    synchronized (lock) {

      if (activeQueriesAndSinks.containsKey(query)) {
        LOG.error("Query {} is already registered!", query);
        return;
      }
      Set<String> sinkIds = query.sinks().stream().map(Named::getId).collect(Collectors.toSet());
      activeQueriesAndSinks.put(query, sinkIds);
    }

  }

  public void deregisterQuery(Query query) {
    if (query == null) {
      LOG.error("Cannot deregister null query");
      throw new IllegalArgumentException("Cannot deregister null query");
    }
    synchronized (lock) {

      if (!activeQueriesAndSinks.containsKey(query)) {
        LOG.error("Query {} is not registered and cannot be deregistered!", query);
        return;
      }
      activeQueriesAndSinks.remove(query);
    }

  }

  public void sinkFinished(Query query, Sink<?> sink) {
    synchronized (lock) {
      if (!activeQueriesAndSinks.containsKey(query)) {
        LOG.error("Notifying a sink as finished for Query {}, which is not registered!", query);
        throw new IllegalArgumentException("Notifying a sink as finished for a Query which is not registered!");
      }
      if (!activeQueriesAndSinks.get(query).contains(sink.getId())) {
        LOG.warn("Sink {} for Query {} reported finished more than once or unknown sink", sink.getId(), query);
        return;
      }
      activeQueriesAndSinks.get(query).remove(sink.getId());
    }
  }

  public void interruptTerminator() {
    terminatorThread.interrupt();
  }
}
