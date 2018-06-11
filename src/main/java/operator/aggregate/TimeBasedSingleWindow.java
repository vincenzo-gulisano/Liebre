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

package operator.aggregate;

import common.tuple.RichTuple;

public interface TimeBasedSingleWindow<T1 extends RichTuple, T2 extends RichTuple> {

	public TimeBasedSingleWindow<T1, T2> factory();

	public void add(T1 t);

	public void remove(T1 t);

	public T2 getAggregatedResult();

	public void setKey(String key);

	public void setStartTimestamp(long startTimestamp);

}
