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

import common.statistic.AvgStat;
import common.tuple.Tuple;

public class SinkStatistic<T extends Tuple> extends BaseSink<T> {

	private BaseSink<T> sink;
	private AvgStat processingTimeStat;

	public SinkStatistic(BaseSink<T> sink, String outputFile) {
		this(sink, outputFile, true);
	}

	public SinkStatistic(BaseSink<T> sink, String outputFile, boolean autoFlush) {
		super(sink.getId(), sink.state.getStreamFactory(), sink.function);
		this.sink = sink;
		this.processingTimeStat = new AvgStat(outputFile, autoFlush);
	}

	@Override
	public void deActivate() {
		processingTimeStat.close();
		super.deActivate();
	}

	public void process() {
		T t = getInputStream(getId()).getNextTuple();
		if (t != null) {
			long start = System.nanoTime();
			this.sink.processTuple(t);
			processingTimeStat.add(System.nanoTime() - start);
		}
	}

	@Override
	public void processTuple(T tuple) {
		throw new UnsupportedOperationException();
	}
}
