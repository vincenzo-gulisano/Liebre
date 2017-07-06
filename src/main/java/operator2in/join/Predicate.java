package operator2in.join;

import tuple.Tuple;

public interface Predicate<T1 extends Tuple, T2 extends Tuple, T3 extends Tuple> {

	public T3 compare(T1 t1, T2 t2);

}
