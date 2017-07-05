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

package operator.filter;

import java.util.LinkedList;
import java.util.List;

import operator.BaseOperator;
import tuple.Tuple;

public class FilterOperator<T extends Tuple> extends BaseOperator<T, T> {

	private FilterFunction<T> filter;

	public FilterOperator(FilterFunction<T> filter) {
		this.filter = filter;
	}

	@Override
	protected void process() {
		T inTuple = in.getNextTuple();
		if (inTuple != null && filter.forward(inTuple)) {
			out.addTuple(inTuple);
		}
	}

	@Override
	protected List<T> processTuple(T tuple) {
		List<T> result = new LinkedList<T>();
		if (filter.forward(tuple)) {
			out.addTuple(tuple);
		}
		return result;
	}

}
