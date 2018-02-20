package common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

public class ExecutionMatrix {
	private final long[][] matrix;
	private ReadWriteLock[] locks;
	private final int nThreads;
	private final int nTasks;

	public ExecutionMatrix(int nTasks, int nThreads) {
		this.matrix = new long[nThreads][nTasks];
		this.locks = new ReadWriteLock[nThreads];
		for (int i = 0; i < nThreads; i++) {
			locks[i] = new ReentrantReadWriteLock();
		}
		this.nThreads = nThreads;
		this.nTasks = nTasks;
	}

	public void init(long value) {
		for (int i = 0; i < nThreads; i++) {
			try {
				locks[i].writeLock().lockInterruptibly();
				try {
					for (int j = 0; j < nTasks; j++) {
						matrix[i][j] = value;
					}
				} finally {
					locks[i].writeLock().unlock();
				}
			} catch (InterruptedException e) {
				System.out.format("[WARN] Lock interrupted: %s%n", e.getStackTrace()[2]);
				// Restore interruption status for thread
				Thread.currentThread().interrupt();
			}

		}
	}

	public void update(Map<String, Long> updates, Map<String, Integer> index, int threadId) {
		for (Map.Entry<String, Long> update : updates.entrySet()) {
			int taskId = index.get(update.getKey());
			try {
				locks[threadId].writeLock().lockInterruptibly();
				try {
					matrix[threadId][taskId] = update.getValue();
				} finally {
					locks[threadId].writeLock().unlock();
				}
			} catch (InterruptedException e) {
				System.out.format("[WARN] Lock interrupted: %s%n", e.getStackTrace()[2]);
				// Restore interruption status for thread
				Thread.currentThread().interrupt();
			}
		}

	}

	private long apply(int taskId, BiFunction<Long, Long, Long> func, long initialValue) {
		long result = initialValue;
		for (int i = 0; i < nThreads; i++) {
			try {
				locks[i].readLock().lockInterruptibly();
				try {
					result = func.apply(result, matrix[i][taskId]);
				} finally {
					locks[i].readLock().unlock();
				}
			} catch (InterruptedException e) {
				System.out.format("[WARN] Lock interrupted: %s%n", e.getStackTrace()[2]);
				// Restore interruption status for thread
				Thread.currentThread().interrupt();
			}
		}
		return result;
	}

	public long sum(int taskId) {
		return apply(taskId, (a, b) -> a + b, 0);
	}

	public long min(int taskId) {
		return apply(taskId, Math::min, matrix[0][taskId]);
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
		for (int i = 0; i < nThreads; i++) {
			for (int j = 0; j < nTasks; j++) {
				sb.append(matrix[i][j]).append(" ");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

}
