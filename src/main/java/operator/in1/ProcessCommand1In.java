package operator.in1;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;

public class ProcessCommand1In<IN extends Tuple, OUT extends Tuple>
		extends AbstractProcessCommand<Operator1In<IN, OUT>> {

	protected ProcessCommand1In(Operator1In<IN, OUT> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		operator.onScheduled();
		IN inTuple = operator.getInputStream(operator.getId()).getNextTuple();
		if (inTuple != null) {
			operator.onRun();
			List<OUT> outTuples = operator.processTupleIn1(inTuple);
			if (outTuples != null) {
				for (OUT t : outTuples)
					operator.getOutputStream(operator.getId()).addTuple(t);
			}
		}
	}

}
