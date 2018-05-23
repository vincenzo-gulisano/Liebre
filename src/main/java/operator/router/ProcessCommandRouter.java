package operator.router;

import java.util.List;

import common.tuple.Tuple;
import java.util.Objects;
import operator.AbstractProcessCommand;
import stream.Stream;

public class ProcessCommandRouter<T extends Tuple> extends
    AbstractProcessCommand<RouterOperator<T>> {

  protected ProcessCommandRouter(RouterOperator<T> operator) {
    super(operator);
  }

  @Override
  public final void process() {
    Stream<T> input = operator.getInput();
    T inTuple = input.getNextTuple();
    if (inTuple != null) {
      metric.recordTupleRead(inTuple, input);
      List<String> streams = operator.chooseOperators(inTuple);
      if (streams != null) {
        for (String op : streams) {
          Stream<T> output = getOutputStream(op);
          metric.recordTupleWrite(inTuple, output);
          output.addTuple(inTuple);
        }
      }
    }
  }

  private Stream<T> getOutputStream(String id) {
    for (Stream<T> stream : operator.getOutputs()) {
      if (Objects.equals(stream.getId(), id)) {
        return stream;
      }
    }
    throw new IllegalStateException(String.format(
        "Requested output stream with id '%s' but operator '%s' has the following outputs: %s", id,
        operator.getId(), operator.getOutputs()));
  }
}
