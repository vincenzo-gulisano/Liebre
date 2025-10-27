package query;

import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

public class TerminationAction implements Runnable {

    private final HashMap<Query, Set<String>> activeQueriesAndSinks;
    private final Object lock;
    
    public TerminationAction(HashMap<Query, Set<String>> activeQueriesAndSinks, Object lock) {
      this.activeQueriesAndSinks = activeQueriesAndSinks;
      this.lock = lock;
    }

    @Override
    public void run() {
      QueryTerminator.LOG.trace("Terminator started");
      while (!Thread.currentThread().isInterrupted() && !activeQueriesAndSinks.isEmpty()) {
        synchronized (lock) {
          if (!activeQueriesAndSinks.isEmpty()) {
            for (Entry<Query, Set<String>> activeQuery : activeQueriesAndSinks.entrySet()) {
              QueryTerminator.LOG.trace("Active Sinks for Query {}: {}", activeQuery.getKey(), activeQuery.getValue());
              if (activeQuery.getValue().isEmpty()) {
                QueryTerminator.LOG.info("All sinks for Query {} have finished. Deactivating query.", activeQuery.getKey());
                activeQuery.getKey().deActivate();
                activeQueriesAndSinks.remove(activeQuery.getKey());
                break; // Break to avoid ConcurrentModificationException
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
      }
    }
  }