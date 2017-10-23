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

package operator.Union;

import java.util.HashMap;
import java.util.List;

import common.tuple.Tuple;
import operator.BaseOperator;
import stream.Stream;

public class UnionOperator<T extends Tuple> extends BaseOperator<T, T> {

	protected HashMap<String, Stream<T>> ins;
	protected boolean active = false;

	public UnionOperator() {
		ins = new HashMap<String, Stream<T>>();
	}

	public void registerIn(String id, Stream<T> in) {
		this.ins.put(id, in);
	}

	@Override
	public void process() {
		for (Stream<T> in : ins.values()) {
			T inTuple = in.getNextTuple();
			if (inTuple != null) {
				out.addTuple(inTuple);
			}
		}
	}

	@Override
	public List<T> processTuple(T tuple) {
		return null;
	}
}
