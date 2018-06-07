package source;

import common.tuple.Tuple;

@FunctionalInterface
public interface SourceFunction<OUT extends Tuple>  {

  public abstract OUT getNextTuple();

}
