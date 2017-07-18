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

package operator.router;

import java.util.List;

import statistic.AvgStat;
import tuple.Tuple;

public class RouterStatisticOperator<T extends Tuple> extends RouterOperator<T> {

	private AvgStat processingTimeStat;

	public RouterStatisticOperator(RouterFunction<T> router, String outputFile) {
		super(router);
		this.processingTimeStat = new AvgStat(outputFile, true);
	}

	public RouterStatisticOperator(RouterFunction<T> router, String outputFile,
			boolean autoFlush) {
		super(router);
		this.processingTimeStat = new AvgStat(outputFile, autoFlush);
	}

	@Override
	public void deActivate() {
		processingTimeStat.close();
		active = false;
	}

	@Override
	public void process() {
		T inTuple = in.getNextTuple();
		if (inTuple != null) {
			long start = System.nanoTime();
			List<String> streams = router.chooseStreams(inTuple);
			processingTimeStat.add(System.nanoTime() - start);
			if (streams != null)
				for (String stream : streams)
					outs.get(stream).addTuple(inTuple);
		}
	}

}
