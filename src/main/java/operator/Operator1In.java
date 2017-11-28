package operator;

import java.util.List;

import common.tuple.Tuple;

public interface Operator1In<IN extends Tuple, OUT extends Tuple> extends Operator<IN, OUT> {
	List<OUT> processTupleIn1(IN tuple);
}
