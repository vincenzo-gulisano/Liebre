package dummy;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import common.util.Util;
import operator.router.RouterFunction;

/**
 * Router operator that copies the input tuple to all the given output streams
 * with a probability relative to its selectivity.
 * 
 * @author palivosd
 *
 */
public class DummyRouterFunction implements RouterFunction<DummyTuple> {

	private final double selectivity;
	private final long sleepMillis;
	private final Random rand;
	private final List<String> chosenStreams;

	public DummyRouterFunction(double selectivity, long sleepMillis, List<String> chosenStreams) {
		this.selectivity = selectivity;
		this.sleepMillis = sleepMillis;
		this.chosenStreams = chosenStreams;
		this.rand = new Random();
	}

	public List<String> chooseStreams(DummyTuple arg0) {
		Util.sleep(sleepMillis);
		if (rand.nextDouble() < selectivity) {
			return chosenStreams;
		}
		return Collections.emptyList();
	}

}
