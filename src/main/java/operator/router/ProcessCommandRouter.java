package operator.router;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;

public class ProcessCommandRouter<T extends Tuple> extends AbstractProcessCommand<RouterOperator<T>> {

	protected ProcessCommandRouter(RouterOperator<T> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		T inTuple = operator.getInputStream(operator.getId()).getNextTuple();
		if (inTuple != null) {
			List<String> streams = operator.chooseOperators(inTuple);
			if (streams != null)
				for (String op : streams)
					operator.getOutputStream(op).addTuple(inTuple);
		}
	}

}
