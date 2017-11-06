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

import common.statistic.CountStat;
import common.tuple.Tuple;

public class StreamStatistic<T extends Tuple> implements Stream<T> {

	private Stream<T> stream;
	private CountStat inRate;
	private CountStat outRate;

	public StreamStatistic(Stream<T> stream, String inRateFile, String outRateFile, boolean autoFlush) {
		this.stream = stream;
		inRate = new CountStat(inRateFile, autoFlush);
		outRate = new CountStat(outRateFile, autoFlush);
	}

	@Override
	public void addTuple(T tuple) {
		inRate.increase(1);
		stream.addTuple(tuple);
	}

	@Override
	public T getNextTuple() {
		T out = stream.getNextTuple();
		if (out != null)
			outRate.increase(1);
		return out;
	}

	@Override
	public void deActivate() {
		// FIXME: Call when deactivating box
		inRate.close();
		outRate.close();
	}

	@Override
	public boolean isActive() {
		return true;
	}

	@Override
	public long size() {
		return stream.size();
	}

	@Override
	public String getId() {
		return stream.getId();
	}

}
