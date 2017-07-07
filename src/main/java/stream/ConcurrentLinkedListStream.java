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

package stream;

import java.util.concurrent.ConcurrentLinkedQueue;

import tuple.Tuple;

public class ConcurrentLinkedListStream<T extends Tuple> implements Stream<T> {

	private ConcurrentLinkedQueue<T> stream = new ConcurrentLinkedQueue<T>();

	@Override
	public void addTuple(T tuple) {
		stream.add(tuple);
	}

	@Override
	public T getNextTuple() {
		return stream.poll();
	}

	@Override
	public void activate() {

	}

	@Override
	public void deActivate() {

	}

}
