package reports;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

// TODO: Delete 
public class Report {
	public static void reportOutput(final String metricName, String units, String path) {
		final File dir = new File(path);
		File[] files = dir.listFiles(new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return name.matches(String.format("O\\d+\\.%s\\.csv", metricName));
			}

		});
		for (File logFile : files) {
			try {
				Reader in = new FileReader(logFile);
				Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
				double total = 0;
				double min = Double.MAX_VALUE;
				double max = Double.MIN_VALUE;
				long measurements = 0;
				for (CSVRecord record : records) {
					double metric = Double.parseDouble(record.get(1));
					if (!Double.isNaN(metric) && metric > 0) {
						if (metric < min) {
							min = metric;
						}
						if (metric > max) {
							max = metric;
						}
						total += metric;
						measurements++;
					}
				}
				double averageMetric = total / measurements;
				if (!Double.isNaN(averageMetric)) {
					String sourceName = logFile.getName().split("\\.")[0];
					System.out.println(String.format("%s min %s = %3.0f %s", sourceName, metricName, min, units));
					System.out.println(
							String.format("%s average %s = %3.0f %s", sourceName, metricName, averageMetric, units));
					System.out.println(String.format("%s max %s = %3.0f %s", sourceName, metricName, max, units));
					System.out.println("------------");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}
