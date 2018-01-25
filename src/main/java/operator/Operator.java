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

package operator;

import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;

public interface Operator<IN extends Tuple, OUT extends Tuple>
		extends ActiveRunnable, StreamConsumer<IN>, StreamProducer<OUT> {

	/**
	 * Heuristic that indicates that the operator has some input <b>all</b> its
	 * input streams. Might not always be accurate in the case of operators with
	 * multiple input streams.
	 * 
	 * @return {@code true} if the operator has some tuples available on all its
	 *         input streams.
	 */
	boolean hasInput();

	/**
	 * Heuristic that indicates if the operator can write tuples to all its output
	 * streams.
	 * 
	 * @return {@code true} if the operator can write tuples to all its output
	 *         streams
	 */
	boolean hasOutput();
}
