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

import common.statistic.AverageStatistic;
import common.tuple.Tuple;
import common.util.StatisticFilename;

public class SinkStatistic<T extends Tuple> extends SinkDecorator<T> {

	private final AverageStatistic processingTimeStat;

	public SinkStatistic(Sink<T> sink, String outputFolder) {
		this(sink, outputFolder, true);
	}

	public SinkStatistic(Sink<T> sink, String outputFolder, boolean autoFlush) {
		super(sink);
		this.processingTimeStat = new AverageStatistic(StatisticFilename.INSTANCE.get(outputFolder, sink, "proc"),
				autoFlush);
	}

	@Override
	public void enable() {
		super.enable();
		processingTimeStat.enable();
	}

	@Override
	public void disable() {
		processingTimeStat.disable();
		super.disable();
	}

	@Override
	public void processTuple(T tuple) {
		long start = System.nanoTime();
		super.processTuple(tuple);
		processingTimeStat.append(System.nanoTime() - start);
	}
}
