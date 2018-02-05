package operator.in2;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;

public class ProcessCommand2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		extends AbstractProcessCommand<Operator2In<IN, IN2, OUT>> {

	public ProcessCommand2In(Operator2In<IN, IN2, OUT> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		IN inTuple1 = operator.getInputStream(operator.getId()).getNextTuple();
		IN2 inTuple2 = operator.getInput2Stream(operator.getId()).getNextTuple();
		if (inTuple1 != null) {
			List<OUT> outTuples = operator.processTupleIn1(inTuple1);
			if (outTuples != null) {
				for (OUT t : outTuples)
					operator.getOutputStream(operator.getId()).addTuple(t);
			}
		}
		if (inTuple2 != null) {
			List<OUT> outTuples = operator.processTupleIn2(inTuple2);
			if (outTuples != null) {
				for (OUT t : outTuples)
					operator.getOutputStream(operator.getId()).addTuple(t);
			}
		}
	}

}
