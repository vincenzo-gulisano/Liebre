/*  Copyright (C) 2017  Vincenzo Gulisano
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package operator.aggregate;

import tuple.RichTuple;

/* Assumptions: (1) all tuples added to an instance of such a 
 * window share the same key and (2) it is the responsibility 
 * of the window to set the key in the output tuple if needed */
public interface TimeBasedSingleWindow<T1 extends RichTuple, T2 extends RichTuple> {

	public TimeBasedSingleWindow<T1, T2> factory(long timestamp, String key);

	public void add(T1 t);

	public void remove(T1 t);

	public T2 getAggregatedResult(double timestamp, T1 triggeringTuple);

	public long size();

}
