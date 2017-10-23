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

package sink.text;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import common.tuple.Tuple;
import sink.BaseSink;

public class TextSink<T extends Tuple> extends BaseSink<T> {

	private boolean closed;

	private PrintWriter pw;
	protected TextSinkFunction<T> function;

	public TextSink(String fileName, TextSinkFunction<T> function,
			boolean autoFlush) {
		closed = false;
		this.function = function;
		try {
			this.pw = new PrintWriter(new FileWriter(fileName), autoFlush);
			closed = false;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write(String s) {
		pw.println(s);
	}

	public void close() {
		if (!closed) {
			pw.flush();
			pw.close();
			closed = true;
		}
	}

	public void deActivate() {
		active = false;
		close();
	}

	@Override
	public void processTuple(T tuple) {
		write(function.convertTupleToLine(tuple));
	}

}
