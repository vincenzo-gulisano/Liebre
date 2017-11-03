package example.data;

import java.io.FileWriter;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * Generate dummy CSV data to test examples.
 * 
 * @author palivosd
 *
 */
public abstract class ExampleTextDataGenerator {

	public void generate(String outputFile) throws Exception {
		CSVPrinter csvFilePrinter = null;
		CSVFormat csvFileFormat = CSVFormat.DEFAULT;
		FileWriter fileWriter = new FileWriter(outputFile);
		csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);

		for (int i = 0; i < numberOfLinesToGenerate(); i++) {
			csvFilePrinter.printRecord(getNextRecord());
		}

		fileWriter.flush();
		fileWriter.close();
		csvFilePrinter.close();
	}

	protected abstract List<String> getNextRecord();

	protected abstract int numberOfLinesToGenerate();

}
