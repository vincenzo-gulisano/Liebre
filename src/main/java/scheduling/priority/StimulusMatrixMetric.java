/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package scheduling.priority;

import java.util.List;

import common.component.Component;
import common.exec.TimestampExecutionMatrix;
import common.exec.TimestampExecutionMatrix.MatrixElement;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public class StimulusMatrixMetric extends PriorityMetric {

	private final TimestampExecutionMatrix matrix;

	private static final int WRITE_TYPE = 0;
	private static final int READ_TYPE = 1;

	public StimulusMatrixMetric(List<Component> tasks, List<Component> passiveTasks, int nThreads) {
		super(tasks, passiveTasks, nThreads);
		this.matrix = new TimestampExecutionMatrix(this.maximumStreamIndex, nThreads);
		System.err.println("[WARN] STIMULUS MATRIX NOT SUPPORTED AT THE MOMENT!");
	}

	@Override
	public List<Double> getPriorities(int scaleFactor) {
		MatrixElement[] latest = matrix.latest();
		long[] priorities = new long[tasks.size()];
		for (int i = 0; i < tasks.size(); i++) {
			priorities[i] = getTaskPriority(tasks.get(i), latest);
		}
		preprocessTimestamps(priorities);
		List<Double> res = scale(priorities, scaleFactor);
		return res;
	}

	private long getTaskPriority(Component task, MatrixElement[] latest) {
		if (isIgnored(task)) {
			return 0;
		}
		MatrixElement elem = new MatrixElement();
		for (int idx : getInputIndexes(task)) {
			elem = MatrixElement.getOldest(elem, latest[idx]);
		}
		return elem.value;
	}

	/**
	 * Timestamps -> time elapsed
	 * 
	 * @param timestamps
	 */
	private void preprocessTimestamps(long[] timestamps) {
		long t = System.currentTimeMillis();
		for (int i = 0; i < timestamps.length; i++) {
			long ts = timestamps[i];
			ts = (ts > 0) ? (t - ts) : 0;
			timestamps[i] = ts;
		}
	}

	@Override
	public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
		matrix.put(threadIndex(), input.getIndex(), ((RichTuple) tuple).getStimulus(), READ_TYPE);
	}

	@Override
	public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
		matrix.put(threadIndex(), output.getIndex(), ((RichTuple) tuple).getStimulus(), WRITE_TYPE);
	}

}
