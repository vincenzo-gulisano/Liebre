package experimental.provenance;

import component.operator.in1.map.MapFunction;

public class GenealogMapFunction<T1 extends GenealogTuple, T2 extends GenealogTuple>
    implements MapFunction<T1, T2> {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();
  private MapFunction<T1, T2> function;

  public GenealogMapFunction(MapFunction<T1, T2> function) {
    this.function = function;
  }

  @Override
  public T2 apply(T1 t) {
    T2 result = function.apply(t);

    if (result != null) {
      result.U1 = t;
      result.type = GenealogTupleType.MAP;
      result.setUID(UID.increaseAndGet());
    }

    return result;
  }
}
