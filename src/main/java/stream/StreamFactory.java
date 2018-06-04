package stream;

import common.component.Component;
import common.tuple.Tuple;

public interface StreamFactory {

  <T extends Tuple> Stream<T> newBoundedStream(Component from, Component to, int capacity);

}
