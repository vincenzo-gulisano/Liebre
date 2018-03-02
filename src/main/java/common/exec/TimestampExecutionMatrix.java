package common.exec;

import java.util.concurrent.atomic.AtomicReferenceArray;

public class TimestampExecutionMatrix extends ExecutionMatrix {
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

	/**
	 * Construct
	 * 
	 * @param nTasks
	 * @param nThreads
	 */
	public TimestampExecutionMatrix(int nTasks, int nThreads) {
		super(nTasks, nThreads);
		this.matrix = new AtomicReferenceArray<MatrixElement>(nTasks * nThreads);
		initMatrix();
	}

	private void initMatrix() {
		for (int i = 0; i < nThreads; i++) {
			for (int j = 0; j < nTasks; j++) {
				set(i, j, new MatrixElement());
			}
		}
	}

	private MatrixElement get(int threadId, int taskId) {
		return matrix.get(getIndex(threadId, taskId));
	}

	private void set(int threadId, int taskId, MatrixElement element) {
		matrix.set(getIndex(threadId, taskId), element);
	}

	public void put(int threadId, int taskId, long value) {
		set(threadId, taskId, new MatrixElement(value));
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

	public long[] latest() {
		long[] result = new long[nTasks];
		for (int i = 0; i < nTasks; i++) {
			result[i] = latest(i);
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
