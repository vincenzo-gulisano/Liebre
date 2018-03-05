package operator;

import common.exec.ProcessCommand;
import scheduling.priority.PriorityMetric;

/**
 * Encapsulation of the execution logic for operators. This is required in order
 * to have reusable decorators without the need to duplicate code (i.e. the
 * process() function) and without having to resort to method interceptors.
 * <br/>
 * Note that similar classes exist for sources and sinks but for technical
 * reasons do not extend this.
 * 
 * @author palivosd
 *
 * @param <OP>
 *            The operator subclass used.
 */
public abstract class AbstractProcessCommand<OP extends Operator<?, ?>> implements ProcessCommand {
	protected final OP operator;
	protected PriorityMetric metric = PriorityMetric.noopMetric();

	protected AbstractProcessCommand(OP operator) {
		this.operator = operator;
	}

	@Override
	public final void run() {
		if (operator.isEnabled()) {
			process();
		}
	}

	@Override
	public abstract void process();

	public void setMetric(PriorityMetric metric) {
		this.metric = metric;
	}

}
