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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import common.tuple.Tuple;
import common.util.Util;

public abstract class TextSourceFunction<T extends Tuple> implements SourceFunction<T> {

	private BufferedReader br;
	private String nextLine = "";
	private boolean hasNext = true;

	public TextSourceFunction(String fileName) {
		try {
			this.br = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public boolean hasNext() {
		if (hasNext) {
			try {
				if ((nextLine = br.readLine()) == null) {
					br.close();
					hasNext = false;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// Prevent spinning
			Util.sleep(1000);
		}
		return hasNext;
	}

	@Override
	public T getNextTuple() {
		if (hasNext()) {
			return getNext(nextLine);
		} else {
			return null;
		}
	}

	protected abstract T getNext(String line);

	@Override
	public void deActivate() {
		if (hasNext) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
