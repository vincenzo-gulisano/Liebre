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
import stream.Stream;
import tuple.Tuple;

public class SourceStatistic<T extends Tuple> implements Source<T> {

	protected Stream<T> out;
	protected boolean active = false;
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
	public void registerOut(Stream<T> out) {
		this.out = out;
		this.source.registerOut(out);
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
		processingTimeStat.close();
		active = false;
	}

	protected void process() {
		long start = System.nanoTime();
		this.source.process();
		processingTimeStat.add(System.nanoTime() - start);
	}
}
