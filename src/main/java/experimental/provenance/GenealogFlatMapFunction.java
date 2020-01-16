package experimental.provenance;

import component.operator.in1.map.FlatMapFunction;
import java.util.List;

public class GenealogFlatMapFunction<T1 extends GenealogTuple, T2 extends GenealogTuple>
    implements FlatMapFunction<T1, T2> {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();
  private FlatMapFunction<T1, T2> function;

  public GenealogFlatMapFunction(FlatMapFunction<T1, T2> function) {
    this.function = function;
  }

  public List<T2> apply(T1 tuple) {
    List<T2> result = function.apply(tuple);

    if (result != null && !result.isEmpty()) {
      for (T2 ot : result) {
        ot.U1 = tuple;
        ot.type = GenealogTupleType.MAP;
        ot.setUID(UID.increaseAndGet());
      }
    }

    return result;
  }
}
