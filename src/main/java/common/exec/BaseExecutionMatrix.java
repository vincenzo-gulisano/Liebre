package common.exec;

import java.util.concurrent.atomic.AtomicLongArray;

public class BaseExecutionMatrix extends ExecutionMatrix {

	private final AtomicLongArray matrix;

	public BaseExecutionMatrix(int nTasks, int nThreads) {
		super(nTasks, nThreads);
		matrix = new AtomicLongArray(nTasks * nThreads);
	}

	private long get(int threadId, int taskId) {
		return matrix.get(getIndex(threadId, taskId));
	}

	private void set(int threadId, int taskId, long value) {
		matrix.set(getIndex(threadId, taskId), value);
	}

	public void add(int threadId, int taskId, long value) {
		long currentValue = get(threadId, taskId);
		set(threadId, taskId, currentValue + value);
	}

	private long sum(int taskId) {
		long result = 0;
		for (int threadId = 0; threadId < nThreads; threadId++) {
			result += get(threadId, taskId);
		}
		return result;
	}

	public long[] sum(long minValue) {
		long[] result = new long[nTasks];
		for (int i = 0; i < nTasks; i++) {
			result[i] = Math.max(sum(i), minValue);
		}
		return result;
	}

}
