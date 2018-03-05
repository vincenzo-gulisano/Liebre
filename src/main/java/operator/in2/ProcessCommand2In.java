package operator.in2;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;
import stream.Stream;

public class ProcessCommand2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		extends AbstractProcessCommand<Operator2In<IN, IN2, OUT>> {

	public ProcessCommand2In(Operator2In<IN, IN2, OUT> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		Stream<IN> input1 = operator.getInputStream(operator.getId());
		Stream<IN2> input2 = operator.getInput2Stream(operator.getId());
		Stream<OUT> output = operator.getOutputStream(operator.getId());

		IN inTuple1 = input1.getNextTuple();
		IN2 inTuple2 = input2.getNextTuple();
		if (inTuple1 != null) {
			metric.recordTupleRead(inTuple1, input1);
			List<OUT> outTuples = operator.processTupleIn1(inTuple1);
			if (outTuples != null) {
				for (OUT t : outTuples) {
					metric.recordTupleWrite(t, output);
					output.addTuple(t);
				}
			}
		}
		if (inTuple2 != null) {
			metric.recordTupleRead(inTuple2, input2);
			List<OUT> outTuples = operator.processTupleIn2(inTuple2);
			if (outTuples != null) {
				for (OUT t : outTuples) {
					metric.recordTupleWrite(t, output);
					output.addTuple(t);
				}
			}
		}
	}

}
