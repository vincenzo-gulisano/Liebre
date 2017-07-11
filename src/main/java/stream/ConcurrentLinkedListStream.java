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
import util.BackOff;

public class ConcurrentLinkedListStream<T extends Tuple> implements Stream<T> {

	private ConcurrentLinkedQueue<T> stream = new ConcurrentLinkedQueue<T>();
	private BackOff writerBackOff, readerBackOff;
	private long tuplesWritten, tuplesRead;

	public ConcurrentLinkedListStream() {
		writerBackOff = new BackOff(1, 20, 5);
		readerBackOff = new BackOff(1, 20, 5);
		tuplesWritten = 0;
		tuplesRead = 0;
	}

	@Override
	public void addTuple(T tuple) {
		if (tuplesWritten - tuplesRead > 10000)
			writerBackOff.backoff();
		stream.add(tuple);
		tuplesWritten++;
	}

	@Override
	public T getNextTuple() {
		T nextTuple = stream.poll();
		if (nextTuple == null)
			readerBackOff.backoff();
		else
			tuplesRead++;
		return stream.poll();
	}

	@Override
	public void activate() {

	}

	@Override
	public void deActivate() {

	}

}
