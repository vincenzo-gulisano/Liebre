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

import common.tuple.Tuple;

public class OperatorKey<IN extends Tuple, OUT extends Tuple> {

	final String identifier;
	final Class<IN> type1;
	final Class<OUT> type2;

	public OperatorKey(String identifier, Class<IN> type1, Class<OUT> type2) {
		this.identifier = identifier;
		this.type1 = type1;
		this.type2 = type2;
	}

}
