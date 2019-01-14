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

package operator.aggregate;

import common.tuple.RichTuple;

public abstract class BaseTimeBasedSingleWindow<T1 extends RichTuple, T2 extends RichTuple>
		implements TimeBasedSingleWindow<T1, T2> {

	protected String key;
	protected long startTimestamp;

	@Override
	public abstract TimeBasedSingleWindow<T1, T2> factory();

	@Override
	public abstract void add(T1 t);

	@Override
	public abstract void remove(T1 t);

	@Override
	public abstract T2 getAggregatedResult();

	@Override
	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public void setStartTimestamp(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}

}
