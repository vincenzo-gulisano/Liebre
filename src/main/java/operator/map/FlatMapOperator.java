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

package operator.map;

import java.util.List;

import operator.BaseOperator;
import tuple.Tuple;

public class FlatMapOperator<T1 extends Tuple, T2 extends Tuple> extends
		BaseOperator<T1, T2> {

	private FlatMapFunction<T1, T2> map;

	public FlatMapOperator(FlatMapFunction<T1, T2> map) {
		this.map = map;
	}

	@Override
	protected void process() {
		T1 inTuple = in.getNextTuple();
		if (inTuple != null) {
			List<T2> outTuples = map.map(inTuple);
			for (T2 outTuple : outTuples) {
				out.addTuple(outTuple);
			}
		}
	}

	@Override
	protected List<T2> processTuple(T1 tuple) {
		return null;
	}
}
