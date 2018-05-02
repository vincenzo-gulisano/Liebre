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

import statistic.AvgStat;
import tuple.Tuple;

public class SourceStatistic<T extends Tuple> extends BaseSource<T> {

	private BaseSource<T> source;
	private AvgStat processingTimeStat;

	public SourceStatistic(BaseSource<T> source, String outputFile) {
		this.source = source;
		this.processingTimeStat = new AvgStat(outputFile, true);
	}

	public SourceStatistic(BaseSource<T> source, String outputFile,
			boolean autoFlush) {
		this.source = source;
		this.processingTimeStat = new AvgStat(outputFile, autoFlush);
	}

	@Override
	public void activate() {
		//FIXME: Broken decoration logic
		source.activate();
		super.activate();
	}

	@Override
	public void deActivate() {
		//FIXME: Broken decoration logic
		processingTimeStat.close();
		active = false;
		source.deActivate();
	}

	public void process() {
		long start = System.nanoTime();
		T t = this.source.getNextTuple();
		processingTimeStat.add(System.nanoTime() - start);
		if (t != null)
			out.addTuple(t);
	}

	@Override
	public T getNextTuple() {
		return null;
	}
}
