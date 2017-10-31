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

package source;

import java.util.Arrays;
import java.util.List;

import common.StreamConsumer;
import common.tuple.Tuple;
import stream.Stream;

public abstract class BaseSource<T extends Tuple> implements Source<T> {

	protected StreamConsumer<T> next;
	protected boolean active = false;
	protected final String id;

	public BaseSource(String id) {
		this.id = id;
	}

	@Override
	public void registerOut(StreamConsumer<T> out) {
		if (active) {
			throw new IllegalStateException();
		}
		this.next = out;
		out.registerIn(this);
	}

	@Override
	public Stream<T> getOutputStream(String reqId) {
		return next.getInputStream(reqId);
	}

	@Override
	public void run() {
		while (active) {
			process();
		}
	}

	@Override
	public void activate() {
		if (next == null) {
			throw new IllegalStateException(id);
		}
		active = true;
	}

	@Override
	public List<StreamConsumer<T>> getNext() {
		return Arrays.asList(this.next);
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
		T t = getNextTuple();
		if (t != null)
			getOutputStream(getId()).addTuple(t);
	}

	@Override
	public String getId() {
		return this.id;
	}

	public abstract T getNextTuple();
}
