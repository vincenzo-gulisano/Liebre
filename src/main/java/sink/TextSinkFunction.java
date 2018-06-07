package sink;

import common.tuple.Tuple;

@FunctionalInterface
public interface TextSinkFunction<T extends Tuple> {

  String processTuple(T tuple);

}
