package operator.in1;

import java.util.List;

import common.tuple.Tuple;
import operator.Operator;

public interface Operator1In<IN extends Tuple, OUT extends Tuple> extends Operator<IN, OUT> {
	List<OUT> processTupleIn1(IN tuple);
}
