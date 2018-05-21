package source;

import common.component.ProcessCommand;
import common.tuple.Tuple;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class ProcessCommandSource<T extends Tuple> implements ProcessCommand {
	private final Source<T> source;
	private PriorityMetric metric = PriorityMetric.noopMetric();

	public ProcessCommandSource(Source<T> source) {
		this.source = source;
	}

	@Override
	public void run() {
		if (source.isEnabled()) {
			process();
		}
	}

	@Override
	public final void process() {
		T tuple = source.getNextTuple();
		if (tuple != null) {
			Stream<T> output = source.getOutputStream(source.getId());
			metric.recordTupleWrite(tuple, output);
			output.addTuple(tuple);
		}
	}

	public void setMetric(PriorityMetric metric) {
		this.metric = metric;
	}

}
