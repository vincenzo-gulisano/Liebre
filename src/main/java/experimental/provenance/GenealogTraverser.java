package experimental.provenance;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

//FIXME: Incorporate any fixes from Flink version
public enum GenealogTraverser {
  INSTANCE;

  public Set<GenealogTuple> process(GenealogTuple t) {
    return traverseGraph(t);
  }

  private Set<GenealogTuple> traverseGraph(GenealogTuple start) {
    // LinkedHashSet to preserve insertion order
    Set<GenealogTuple> result = new LinkedHashSet<>();
    Set<GenealogTuple> visited = new HashSet<>();
    ArrayDeque<GenealogTuple> queue = new ArrayDeque<>();
    queue.addLast(start);
    while (!queue.isEmpty()) {
      GenealogTuple t = queue.removeFirst();
      switch (t.type) {
        case SOURCE:
        case REMOTE:
          result.add(t);
          break;
        case MAP:
          addLastIfNotVisited(t.U1, queue, visited);
          break;
        case JOIN:
          addLastIfNotVisited(t.U1, queue, visited);
          addLastIfNotVisited(t.U2, queue, visited);
          break;
        case AGGREGATE:
          addLastIfNotVisited(t.U2, queue, visited);
          GenealogTuple temp = t.U2.N;
          while (temp != null && temp != t.U1) {
            addLastIfNotVisited(temp, queue, visited);
            temp = temp.N;
          }
          addLastIfNotVisited(t.U1, queue, visited);
          break;
        default:
          throw new IllegalStateException("Unknown DTETupleType " + t.type);
      }
    }
    return result;
  }

  private void addLastIfNotVisited(
      GenealogTuple t, ArrayDeque<GenealogTuple> queue, Set<GenealogTuple> visited) {
    if (t == null) {
      throw new IllegalArgumentException("Null pointer in provenance graph!");
    }
    if (!visited.contains(t)) {
      visited.add(t);
      queue.addLast(t);
    }
  }
}
