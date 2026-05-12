package query;

import component.Component;
import component.source.Source;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Export a simple YAML representation of the query graph, presenting each query
 * Component with its
 * downstream components. For example:
 *
 * <pre>
 * SOURCE:
 *   - A
 *   - B
 *  A:
 *   - C
 *  B:
 *   - C
 *  C:
 *   - SINK
 * SINK:
 * </pre>
 * 
 * Represents a SOURCE feeding operators A and B, both of which are connected to
 * C, which in turn is
 * connected to the SINK.
 */
public class QueryGraphExporter {

  private static final Logger LOGGER = LogManager.getLogger(QueryGraphExporter.class);

  public static void exportAsJson(Query query, String path) {
    final Map<String, Set<String>> downstreamEdges = new HashMap<>();
    final Queue<Component> queue = new ArrayDeque<>();
    final Set<String> visited = new HashSet<>();
    for (Source<?> source : query.sources()) {
      queue.add(source);
    }
    while (!queue.isEmpty()) {
      Component component = queue.remove();
      visit(component, visited, downstreamEdges);
      for (Component child : component.getDownstream()) {
        queue.add(child);
      }
    }
    try (PrintWriter writer = new PrintWriter(path)) {
      // Export into simple YAML format of node -> {downstream nodes}
      for (String upstream : downstreamEdges.keySet()) {
        writer.format("%s:\n", upstream);
        downstreamEdges.get(upstream).forEach(
            downstream -> writer.format("\t- %s\n", downstream));
        writer.println();
      }
      writer.flush();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static void printFlushingState(Query query) {
    class FlushingStatePrinter {

      void print(component.StreamProducer<?> producer, int depth, Set<String> path) {
        boolean hasOutputs = false;
        for (stream.Stream<?> output : producer.getOutputs()) {
          hasOutputs = true;
          for (component.StreamConsumer<?> consumer : output.consumers()) {
            LOGGER.info("{}{} --> {} ({})",
                indentation(depth),
                producer.getId(),
                consumer.getId(),
                output.isFlushed() ? "flushed" : "not flushed");
            if (consumer instanceof component.StreamProducer<?>) {
              component.StreamProducer<?> downstreamProducer =
                  (component.StreamProducer<?>) consumer;
              if (path.add(downstreamProducer.getId())) {
                print(downstreamProducer, depth + 1, path);
                path.remove(downstreamProducer.getId());
              }
            }
          }
        }
        if (!hasOutputs) {
          LOGGER.info("{}{} (no outputs)", indentation(depth), producer.getId());
        }
      }

      String indentation(int depth) {
        StringBuilder indentation = new StringBuilder();
        for (int i = 0; i < depth; i++) {
          indentation.append("   ");
        }
        return indentation.toString();
      }
    }

    FlushingStatePrinter printer = new FlushingStatePrinter();
    for (Source<?> source : query.sources()) {
      Set<String> path = new HashSet<>();
      path.add(source.getId());
      printer.print(source, 0, path);
    }
  }

  private static void visit(Component component, Set<String> visited,
      Map<String, Set<String>> nodes) {
    if (!visited.add(component.getId())) {
      return;
    }
    Set<String> downstream = nodes.computeIfAbsent(component.getId(), k -> new HashSet<>());
    component.getDownstream().forEach(c -> downstream.add(c.getId()));
  }

  private QueryGraphExporter() {
    // Prevent instantiation
  }

}
