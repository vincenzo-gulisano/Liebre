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

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import common.tuple.Tuple;

public abstract class TextSinkFunction<T extends Tuple> implements SinkFunction<T> {
	private boolean closed;
	private PrintWriter pw;

	protected TextSinkFunction(String filename) {
		this(filename, true);
	}

	protected TextSinkFunction(String filename, boolean autoflush) {
		try {
			this.pw = new PrintWriter(new FileWriter(filename), autoflush);
			closed = false;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public final void processTuple(T tuple) {
		if (!isActive()) {
			throw new IllegalStateException("Output stream is closed");
		}
		pw.println(processTupleToText(tuple));

	}

	@Override
	public boolean isActive() {
		return !closed;
	}

	protected abstract String processTupleToText(T tuple);

	public void deActivate() {
		if (!closed) {
			pw.flush();
			pw.close();
			closed = true;
		}
	}

}
