package experimental.provenance;

public class ProvenanceTransformationContext {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();

  public <T extends GenealogTuple> T markSource(T tuple) {
    if (tuple == null) {
      return null;
    }
    tuple.type = GenealogTupleType.SOURCE;
    if (tuple.getUID() == null) {
      tuple.setUID(UID.increaseAndGet());
    }
    return tuple;
  }

  public <IN extends GenealogTuple, OUT extends GenealogTuple> OUT markMap(OUT output, IN input) {
    return markDerived(output, GenealogTupleType.MAP, input, null);
  }

  public <LEFT extends GenealogTuple, RIGHT extends GenealogTuple, OUT extends GenealogTuple>
      OUT markJoin(OUT output, LEFT left, RIGHT right) {
    if (right.getTimestamp() > left.getTimestamp()) {
      return markDerived(output, GenealogTupleType.JOIN, right, left);
    }
    return markDerived(output, GenealogTupleType.JOIN, left, right);
  }

  public <OUT extends GenealogTuple> OUT markAggregate(
      OUT output, GenealogTuple first, GenealogTuple last) {
    return markDerived(output, GenealogTupleType.AGGREGATE, last, first);
  }

  public <T extends GenealogTuple> T linkNext(GenealogTuple previous, T next) {
    if (previous != null) {
      previous.N = next;
    }
    return next;
  }

  public TupleChain newTupleChain() {
    return new TupleChain();
  }

  public <OUT extends GenealogTuple> OUT markDerived(
      OUT output, GenealogTupleType type, GenealogTuple firstParent, GenealogTuple secondParent) {
    if (output == null) {
      return null;
    }
    output.U1 = firstParent;
    output.U2 = secondParent;
    output.type = type;
    output.setUID(UID.increaseAndGet());
    return output;
  }

  public static class TupleChain {

    private GenealogTuple first;
    private GenealogTuple last;

    public <T extends GenealogTuple> T append(T tuple) {
      if (tuple == null) {
        return null;
      }
      if (first == null) {
        first = tuple;
      } else {
        last.N = tuple;
      }
      last = tuple;
      return tuple;
    }

    public GenealogTuple first() {
      return first;
    }

    public GenealogTuple last() {
      return last;
    }

    public boolean isEmpty() {
      return first == null;
    }
  }
}
