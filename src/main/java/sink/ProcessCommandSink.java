package sink;

import common.component.ProcessCommand;
import common.tuple.Tuple;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class ProcessCommandSink<T extends Tuple> implements ProcessCommand {

	private final Sink<T> sink;
	private PriorityMetric metric = PriorityMetric.noopMetric();

	public ProcessCommandSink(Sink<T> sink) {
		this.sink = sink;
	}

	@Override
	public void run() {
		if (sink.isEnabled()) {
			process();
		}
	}

	@Override
	public final void process() {
		Stream<T> input = sink.getInput();
		T tuple = input.getNextTuple();
		if (tuple != null) {
			metric.recordTupleRead(tuple, input);
			sink.processTuple(tuple);
		}
	}

	public void setMetric(PriorityMetric metric) {
		this.metric = metric;
	}

}
