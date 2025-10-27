package query;

import component.sink.Sink;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import common.Named;

class QueryTerminator {

  static final Logger LOG = LogManager.getLogger(QueryTerminator.class);
  static final int TERMINATOR_POLL_INTERVAL_MILLIS = 1000;

  private final HashMap<Query, Set<String>> activeQueriesAndSinks = new HashMap<>();
  private final Thread terminatorThread;

  /** Protects all accesses to activeQueriesAndSinks */
  private final Object lock = new Object();

  QueryTerminator() {
    terminatorThread = new Thread(new TerminationAction(activeQueriesAndSinks, lock),
        "QueryTerminatorThread");
    terminatorThread.start();
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
