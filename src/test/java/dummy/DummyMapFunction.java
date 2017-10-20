package dummy;

import java.util.Random;

import operator.map.MapFunction;
import util.Util;

/**
 * Dummy map operator that sleeps for the given amount of time and then outputs
 * a tuple with a probability relative to its selectivity value.
 * 
 * @author palivosd
 *
 */
public class DummyMapFunction implements MapFunction<DummyTuple, DummyTuple> {

	private final double selectivity;
	private final long sleepMillis;
	private final Random rand;

	public DummyMapFunction(double selectivity, long sleepMillis) {
		this.selectivity = selectivity;
		this.sleepMillis = sleepMillis;

		this.rand = new Random();
	}

	public DummyTuple map(DummyTuple arg0) {
		Util.sleep(sleepMillis);
		if (rand.nextDouble() < selectivity) {
			return arg0;
		}
		return null;
	}

}
