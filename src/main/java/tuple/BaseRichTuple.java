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

package tuple;

public class BaseRichTuple implements RichTuple {

	private static final long serialVersionUID = -1096134766842666299L;
	protected long timestamp;
	protected String key;

	public BaseRichTuple(long timestamp, String key) {
		this.timestamp = timestamp;
		this.key = key;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String getKey() {
		return key;
	}
}
