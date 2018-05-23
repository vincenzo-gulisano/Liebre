package operator.in1;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;
import stream.Stream;

public class ProcessCommand1In<IN extends Tuple, OUT extends Tuple>
		extends AbstractProcessCommand<Operator1In<IN, OUT>> {

	protected ProcessCommand1In(Operator1In<IN, OUT> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		Stream<IN> input = operator.getInput();
		Stream<OUT> output = operator.getOutput();

		IN inTuple = input.getNextTuple();
		if (inTuple != null) {
			metric.recordTupleRead(inTuple, input);
			List<OUT> outTuples = operator.processTupleIn1(inTuple);
			if (outTuples != null) {
				for (OUT t : outTuples) {
					metric.recordTupleWrite(t, output);
					output.addTuple(t);
				}
			}
		}
	}

}
