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
