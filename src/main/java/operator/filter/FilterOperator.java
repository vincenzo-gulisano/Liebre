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

import common.tuple.Tuple;
import operator.in1.BaseOperator1In;
import stream.StreamFactory;

public class FilterOperator<T extends Tuple> extends BaseOperator1In<T, T> {

	protected FilterFunction<T> filter;

	public FilterOperator(String id, StreamFactory streamFactory, FilterFunction<T> filter) {
		super(id, streamFactory);
		this.filter = filter;
	}

	@Override
	public void enable() {
		super.enable();
		filter.enable();
	}

	@Override
	public void disable() {
		filter.disable();
		super.disable();
	}

	@Override
	public List<T> processTupleIn1(T tuple) {
		List<T> result = new LinkedList<T>();
		if (filter.forward(tuple))
			result.add(tuple);
		return result;
	}
}
