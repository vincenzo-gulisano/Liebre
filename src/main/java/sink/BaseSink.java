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

package sink;

import common.tuple.Tuple;
import stream.Stream;

public abstract class BaseSink<T extends Tuple> implements Sink<T> {

	protected Stream<T> in;
	protected boolean active = false;

	public BaseSink() {
	}

	@Override
	public void registerIn(String id, Stream<T> in) {
		this.in = in;
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

	@Override
	public boolean isActive() {
		return active;
	}

	public void process() {
		T t = in.getNextTuple();
		if (t != null) {
			processTuple(t);
		}
	}

	public abstract void processTuple(T tuple);
}
