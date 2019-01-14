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
