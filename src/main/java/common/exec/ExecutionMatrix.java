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

//FIXME: Extract common interface
//FIXME: Rename "Tasks" to something more generic
abstract class ExecutionMatrix {

	protected final int nThreads;
	protected final int nTasks;

	protected ExecutionMatrix(int nTasks, int nThreads) {
		this.nThreads = nThreads;
		this.nTasks = nTasks;
	}

	protected int getIndex(int threadId, int taskId) {
		return (nTasks * threadId) + taskId;
	}
}
