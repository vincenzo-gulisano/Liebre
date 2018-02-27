package common.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

//TODO: Make interface and two implementations, one with diffs (without timestamps) and one with latest
public class ExecutionMatrix {

	private static class MatrixElement {
		private final boolean hasValue;
		private final long timestamp;
		private final long value;

		public MatrixElement() {
			this.hasValue = false;
			this.timestamp = 0;
			this.value = 0;
		}

		public MatrixElement(long value) {
			this(System.nanoTime(), value);
		}

		public MatrixElement(long timestamp, long value) {
			this.timestamp = timestamp;
			this.value = value;
			this.hasValue = true;
		}

	}

	private final AtomicReferenceArray<MatrixElement> matrix;
	private final int nThreads;
	private final int nTasks;

	/**
	 * Construct
	 * 
	 * @param nTasks
	 * @param nThreads
	 */
	public ExecutionMatrix(int nTasks, int nThreads) {
		this.matrix = new AtomicReferenceArray<MatrixElement>(nTasks * nThreads);
		this.nThreads = nThreads;
		this.nTasks = nTasks;
		init();
	}

	private void init() {
		for (int i = 0; i < nThreads; i++) {
			for (int j = 0; j < nTasks; j++) {
				set(i, j, new MatrixElement());
			}
		}
	}

	protected MatrixElement get(int threadId, int taskId) {
		return matrix.get((nTasks * threadId) + taskId);
	}

	protected void set(int threadId, int taskId, MatrixElement element) {
		matrix.set((nTasks * threadId) + taskId, element);

	}

	public void add(int threadId, int taskId, long value) {
		MatrixElement e = get(threadId, taskId);
		set(threadId, taskId, new MatrixElement(e.value + value));
	}

	public void put(int threadId, int taskId, long value) {
		set(threadId, taskId, new MatrixElement(value));
	}

	private long sum(int taskId) {
		long result = 0;
		for (int i = 0; i < nThreads; i++) {
			MatrixElement elem = get(i, taskId);
			if (elem.hasValue) {
				result += elem.value;
			}
		}
		return result;
	}

	private long latest(int taskId) {
		long latestTs = 0;
		long result = 0;
		for (int i = 0; i < nThreads; i++) {
			MatrixElement elem = get(i, taskId);
			if (elem.hasValue && elem.timestamp > latestTs) {
				result = elem.value;
				latestTs = elem.timestamp;
			}
		}
		return result;
	}

	public List<Long> sum(long minValue) {
		List<Long> result = new ArrayList<>(nTasks);
		for (int i = 0; i < nTasks; i++) {
			result.add(Math.max(sum(i), minValue));
		}
		return result;
	}

	public List<Long> latest() {
		List<Long> result = new ArrayList<>(nTasks);
		for (int i = 0; i < nTasks; i++) {
			result.add(latest(i));
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("TN ");
		sb.append("\n");
		for (int i = 0; i < nThreads; i++) {
			sb.append("T").append(i).append(" ");
			for (int j = 0; j < nTasks; j++) {
				sb.append(String.format("% 20d", get(i, j).value)).append(" ");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

}
