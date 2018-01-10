package operator;

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
public abstract class AbstractProcessCommand<OP extends Operator<?, ?>> implements Runnable {
	protected final OP operator;

	protected AbstractProcessCommand(OP operator) {
		this.operator = operator;
	}

	@Override
	public final void run() {
		if (operator.isEnabled()) {
			process();
		}
	}

	protected abstract void process();

}
