package common.exec;

import java.util.concurrent.atomic.AtomicReferenceArray;

public class TimestampExecutionMatrix extends ExecutionMatrix {
	public static class MatrixElement {
		private final boolean hasValue;
		private final long timestamp;
		public final long value;
		private final int type;

		public MatrixElement() {
			this.hasValue = false;
			this.timestamp = -1;
			this.value = -1;
			this.type = -1;
		}

		public MatrixElement(long value, int type) {
			this(System.nanoTime(), value, type);
		}

		public MatrixElement(long timestamp, long value, int type) {
			this.timestamp = timestamp;
			this.value = value;
			this.hasValue = true;
			this.type = type;
		}

		public static MatrixElement getOldest(MatrixElement a, MatrixElement b) {
			if (b.hasValue && b.type >= a.type && b.timestamp > a.timestamp) {
				return b;
			} else {
				return a;
			}
		}

		@Override
		public String toString() {
			return String.format("(%b, %d, %d, %d)", hasValue, type, timestamp, value);
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

	public void put(int threadId, int taskId, long value, int type) {
		set(threadId, taskId, new MatrixElement(value, type));
	}

	private MatrixElement latest(int taskId) {
		MatrixElement result = new MatrixElement();
		for (int i = 0; i < nThreads; i++) {
			MatrixElement elem = get(i, taskId);
			// If all writes (0) select the write, if even one read, select the read instead
			result = MatrixElement.getOldest(result, elem);
		}
		return result;
	}

	public MatrixElement[] latest() {
		MatrixElement[] result = new MatrixElement[nTasks];
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
