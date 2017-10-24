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

package operator.router;

import java.util.HashMap;
import java.util.List;

import common.tuple.Tuple;
import operator.BaseOperator;
import stream.Stream;

public class RouterOperator<T extends Tuple> extends BaseOperator<T, T> {

	protected RouterFunction<T> router;
	protected HashMap<String, Stream<T>> outs;

	public RouterOperator(String id, RouterFunction<T> router) {
		super(id);
		this.router = router;
		outs = new HashMap<String, Stream<T>>();
	}

	public void registerOut(String id, Stream<T> out) {
		this.outs.put(id, out);
	}

	@Override
	public void process() {
		T inTuple = in.getNextTuple();
		if (inTuple != null) {
			List<String> streams = router.chooseStreams(inTuple);
			if (streams != null)
				for (String stream : router.chooseStreams(inTuple))
					outs.get(stream).addTuple(inTuple);
		}
	}

	@Override
	public List<T> processTuple(T tuple) {
		return null;
	}

}
