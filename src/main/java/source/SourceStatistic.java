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

import common.statistic.AverageStatistic;
import common.tuple.Tuple;
import stream.StreamFactory;

public class SourceStatistic<T extends Tuple> extends SourceDecorator<T> {

	private final AverageStatistic processingTimeStat;

	public SourceStatistic(Source<T> source, StreamFactory streamFactory, String outputFile) {
		this(source, streamFactory, outputFile, true);
	}

	public SourceStatistic(Source<T> source, StreamFactory streamFactory, String outputFile, boolean autoFlush) {
		super(source);
		this.processingTimeStat = new AverageStatistic(outputFile, autoFlush);
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
	public T getNextTuple() {
		long start = System.nanoTime();
		T tuple = super.getNextTuple();
		processingTimeStat.append(System.nanoTime() - start);
		return tuple;
	}

}
