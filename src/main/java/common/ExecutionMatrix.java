package common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BiFunction;

public class ExecutionMatrix {
	private final AtomicLongArray matrix;
	private final Map<String, Integer> index;
	private final int nThreads;
	private final int nTasks;

	public ExecutionMatrix(Map<String, Integer> index, int nTasks, int nThreads) {
		this.index = index;
		this.matrix = new AtomicLongArray(nTasks * nThreads);
		this.nThreads = nThreads;
		this.nTasks = nTasks;
	}

	protected long get(int threadId, int taskId) {
		return matrix.get((nTasks * threadId) + taskId);
	}

	protected void set(int threadId, int taskId, long newValue) {
		matrix.set((nTasks * threadId) + taskId, newValue);
	}

	public void init(long value) {
		for (int i = 0; i < nThreads; i++) {
			for (int j = 0; j < nTasks; j++) {
				set(i, j, value);
			}
		}

	}

	public void updateReplace(Map<String, Long> updates, int threadId) {
		for (Map.Entry<String, Long> update : updates.entrySet()) {
			int taskId = index.get(update.getKey());
			set(threadId, taskId, update.getValue());
		}
	}

	public void updateApply(Map<String, Long> updates, int threadId, BiFunction<Long, Long, Long> func) {
		for (Map.Entry<String, Long> update : updates.entrySet()) {
			int taskId = index.get(update.getKey());
			long oldValue = get(threadId, taskId);
			set(threadId, taskId, func.apply(oldValue, update.getValue()));
		}
	}

	private long apply(int taskId, BiFunction<Long, Long, Long> func, long initialValue) {
		long result = initialValue;
		for (int i = 0; i < nThreads; i++) {
			result = func.apply(result, get(i, taskId));
		}
		return result;
	}

	public long sum(int taskId) {
		return apply(taskId, (a, b) -> a + b, 0);
	}

	public long min(int taskId) {
		return apply(taskId, Math::min, get(0, taskId));
	}

	public List<Long> sum() {
		List<Long> result = new ArrayList<>(nTasks);
		for (int i = 0; i < nTasks; i++) {
			result.add(sum(i));
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
				sb.append(String.format("% 20d", get(i, j))).append(" ");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

}
