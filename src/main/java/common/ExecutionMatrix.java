package common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;

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

		public MatrixElement(long timestamp, long value) {
			this.timestamp = timestamp;
			this.value = value;
			this.hasValue = true;
		}

		public MatrixElement apply(long timestamp, long newValue, BiFunction<Long, Long, Long> func) {
			if (this.hasValue) {
				return new MatrixElement(timestamp, func.apply(this.value, newValue));
			} else {
				return new MatrixElement(timestamp, newValue);
			}
		}

	}

	private final AtomicReferenceArray<MatrixElement> matrix;
	private final Map<String, Integer> index;
	private final int nThreads;
	private final int nTasks;

	public ExecutionMatrix(Map<String, Integer> index, int nTasks, int nThreads) {
		this.index = index;
		this.matrix = new AtomicReferenceArray<MatrixElement>(nTasks * nThreads);
		this.nThreads = nThreads;
		this.nTasks = nTasks;
		init();
	}

	protected MatrixElement get(int threadId, int taskId) {
		return matrix.get((nTasks * threadId) + taskId);
	}

	protected void set(int threadId, int taskId, MatrixElement element) {
		matrix.set((nTasks * threadId) + taskId, element);

	}

	public void init() {
		for (int i = 0; i < nThreads; i++) {
			for (int j = 0; j < nTasks; j++) {
				set(i, j, new MatrixElement());
			}
		}

	}

	public void updateReplace(Map<String, Long> updates, int threadId) {
		long ts = System.nanoTime();
		for (Map.Entry<String, Long> update : updates.entrySet()) {
			Integer taskId = index.get(update.getKey());
			if (taskId != null) {
				set(threadId, taskId, new MatrixElement(ts, update.getValue()));
			}
		}
	}

	public void updateApply(Map<String, Long> updates, int threadId, BiFunction<Long, Long, Long> func) {
		long ts = System.nanoTime();
		for (Map.Entry<String, Long> update : updates.entrySet()) {
			Integer taskId = index.get(update.getKey());
			if (taskId != null) {
				MatrixElement elem = get(threadId, taskId);
				set(threadId, taskId, elem.apply(ts, update.getValue(), func));
			}

		}
	}

	private long apply(int taskId, BiFunction<Long, Long, Long> func, long initialValue) {
		long result = initialValue;
		for (int i = 0; i < nThreads; i++) {
			MatrixElement elem = get(i, taskId);
			if (elem.hasValue) {
				result = func.apply(result, elem.value);
			}
		}
		return result;
	}

	public long sum(int taskId) {
		return apply(taskId, (a, b) -> a + b, 0);
	}

	public long min(int taskId) {
		return apply(taskId, Math::min, get(0, taskId).value);
	}

	public long latest(int taskId) {
		long latestTs = 0;
		long result = -1;
		for (int i = 0; i < nThreads; i++) {
			MatrixElement elem = get(i, taskId);
			if (elem.hasValue && elem.timestamp > latestTs) {
				result = elem.value;
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

	public List<Long> min() {
		List<Long> result = new ArrayList<>(nTasks);
		for (int i = 0; i < nTasks; i++) {
			result.add(min(i));
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
		index.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue())
				.forEach(e -> sb.append(String.format("%1$20s", e.getKey())));
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
