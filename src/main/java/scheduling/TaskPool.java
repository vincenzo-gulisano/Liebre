/*
 * Copyright (C) 2017-2018
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

package scheduling;

import common.Active;

public interface TaskPool<T extends Runnable> extends Active {
	default void register(T task) {
		put(task, -1);
	}

	default void registerPassive(T task) {
		System.out.format("[WARN] [%s] Ignoring registerPassive(%s)%n", getClass().getSimpleName(), task);
	}

	T getNext(int threadId);

	void put(T task, int threadId);

	void setThreadsNumber(int activeThreads);

}
