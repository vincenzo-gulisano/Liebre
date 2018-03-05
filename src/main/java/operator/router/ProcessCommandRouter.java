package operator.router;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;
import stream.Stream;

public class ProcessCommandRouter<T extends Tuple> extends AbstractProcessCommand<RouterOperator<T>> {

	protected ProcessCommandRouter(RouterOperator<T> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		Stream<T> input = operator.getInputStream(operator.getId());
		T inTuple = input.getNextTuple();
		if (inTuple != null) {
			metric.recordTupleRead(inTuple, input);
			List<String> streams = operator.chooseOperators(inTuple);
			if (streams != null)
				for (String op : streams) {
					Stream<T> output = operator.getOutputStream(op);
					metric.recordTupleWrite(inTuple, output);
					output.addTuple(inTuple);
				}
		}
	}

}
