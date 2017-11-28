package operator.router;

import java.util.List;

import common.tuple.Tuple;
import operator.Operator;

public interface RouterOperator<T extends Tuple> extends Operator<T, T> {

	List<String> chooseOperators(T tuple);

}
