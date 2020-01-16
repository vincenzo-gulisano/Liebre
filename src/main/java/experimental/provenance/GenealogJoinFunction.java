package experimental.provenance;

import component.operator.in2.join.JoinFunction;

public class GenealogJoinFunction<
        T1 extends GenealogTuple, T2 extends GenealogTuple, T3 extends GenealogTuple>
    implements JoinFunction<T1, T2, T3> {

  private final IncreasingUID UID = UIDFactory.INSTANCE.newUID();
  private JoinFunction<T1, T2, T3> function;

  public GenealogJoinFunction(JoinFunction<T1, T2, T3> function) {
    this.function = function;
  }

  @Override
  public T3 apply(T1 t1, T2 t2) {
    T3 t = function.apply(t1, t2);
    if (t != null) {
      if (t2.getTimestamp() > t1.getTimestamp()) {
        t.U1 = t2;
        t.U2 = t1;
      } else {
        t.U1 = t1;
        t.U2 = t2;
      }
      t.type = GenealogTupleType.JOIN;
      t.setUID(UID.increaseAndGet());
    }
    return t;
  }
}
