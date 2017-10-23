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

package operator2in;

import tuple.Tuple;

public class Operator2InKey<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple> {

	final String identifier;
	final Class<IN> type1;
	final Class<IN2> type2;
	final Class<OUT> type3;

	public Operator2InKey(String identifier, Class<IN> type1, Class<IN2> type2,
			Class<OUT> type3) {
		this.identifier = identifier;
		this.type1 = type1;
		this.type2 = type2;
		this.type3 = type3;
	}

}
