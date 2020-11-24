package query;

import component.sink.Sink;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class QueryTerminator {

  private static final Logger LOG = LogManager.getLogger();
  private static final int TERMINATOR_POLL_INTERVAL_MILLIS = 5000;

  private final Query activeQuery;
  private final Set<String> activeSinks = ConcurrentHashMap.newKeySet();
  private final Thread terminatorThread;

  QueryTerminator(Query query) {
    activeQuery = query;
    activeSinks.addAll(query.sinks().stream().map(s -> s.getId()).collect(Collectors.toList()));
    terminatorThread = new Thread(new TerminationAction(activeQuery, activeSinks));
    terminatorThread.start();
  }

  public void sinkFinished(Sink<?> sink) {
    activeSinks.remove(sink.getId());
  }

  public void disable() {
    terminatorThread.interrupt();
  }

  private static class TerminationAction implements Runnable {

    private final Query activeQuery;
    private final Set<String> activeSinks;

    public TerminationAction(Query activeQuery, Set<String> activeSinks) {
      this.activeQuery = activeQuery;
      this.activeSinks = activeSinks;
    }

    @Override
    public void run() {
      LOG.trace("Terminator started");
      while (!Thread.currentThread().isInterrupted() && !activeSinks.isEmpty()) {
        LOG.trace("Active Sinks: {}", activeSinks);
        try {
          Thread.sleep(TERMINATOR_POLL_INTERVAL_MILLIS);
        } catch (InterruptedException e) {
          LOG.trace("Terminator exiting");
          return;
        }
      }
      activeQuery.deActivate();
    }
  }
}
