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

package operator;

import java.util.List;

import stream.Stream;
import tuple.Tuple;

public abstract class BaseOperator<T1 extends Tuple, T2 extends Tuple>
		implements Operator<T1, T2> {

	protected Stream<T1> in;
	protected Stream<T2> out;
	protected boolean active = false;

	public BaseOperator() {
	}

	@Override
	public void registerIn(String id, Stream<T1> in) {
		this.in = in;
	}

	@Override
	public void registerOut(String id, Stream<T2> out) {
		this.out = out;
	}

	@Override
	public void run() {
		while (active) {
			process();
		}
	}

	@Override
	public void activate() {
		active = true;
	}

	@Override
	public void deActivate() {
		active = false;
	}

	protected void process() {
		T1 inTuple = in.getNextTuple();
		if (inTuple != null) {
			List<T2> outTuples = processTuple(inTuple);
			if (outTuples != null) {
				for (T2 t : outTuples)
					out.addTuple(t);
			}
		}
	}

	protected abstract List<T2> processTuple(T1 tuple);
}
