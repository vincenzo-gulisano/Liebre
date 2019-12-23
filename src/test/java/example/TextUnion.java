/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package example;

import com.google.inject.Guice;
import component.operator.Operator;
import component.sink.Sink;
import component.source.Source;
import io.palyvos.dcs.common.util.Util;
import io.palyvos.dcs.common.util.backoff.InactiveBackoff;
import java.io.File;
import query.Query;

public class TextUnion {

  public static void main(String[] args) {

    final String reportFolder = args[0];
    final String inputFile1 = args[1];
    final String inputFile2 = args[2];
    final String outputFile = reportFolder + File.separator + "TextUnion.out.csv";

    Guice.createInjector(new ExampleModule());
    Query q = new Query();

    Source<String> i1 = q.addTextFileSource("i1", inputFile1);

    Operator<String, MyTuple> inputReader1 =
        q.addMapOperator(
            "map",
            line -> {
              Util.sleep(100);
              String[] tokens = line.split(",");
              return new MyTuple(
                  Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
            });

    Source<String> i2 = q.addTextFileSource("i2", inputFile2);

    Operator<String, MyTuple> inputReader2 =
        q.addMapOperator(
            "map",
            line -> {
              Util.sleep(100);
              String[] tokens = line.split(",");
              return new MyTuple(
                  Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
            });

    Operator<MyTuple, MyTuple> union = q.addUnionOperator("union");

    Sink<MyTuple> o1 = q.addTextFileSink("o1", outputFile, true);

    q.connect(i1, inputReader1).connect(inputReader1, union);
    q.connect(i2, inputReader2).connect(inputReader2, union, InactiveBackoff.INSTANCE);
    q.connect(union, o1);

    q.activate();
    Util.sleep(120000);
    q.deActivate();
  }
}
