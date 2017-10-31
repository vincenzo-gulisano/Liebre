package query;

import java.io.File;

import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;
import stream.StreamStatistic;

public class ConcurrentLinkedListStreamStatisticFactory implements StreamFactory {

	private final String folder;
	private final String inRateExension;
	private final String outRateExtension;
	private final boolean autoFlush;

	public ConcurrentLinkedListStreamStatisticFactory(String folder, String inRateExtension, String outRateExtension,
			boolean autoFlush) {
		this.folder = folder;
		this.inRateExension = inRateExtension;
		this.outRateExtension = outRateExtension;
		this.autoFlush = autoFlush;
	}

	@Override
	public <T extends Tuple> Stream<T> newStream(String fromId, String toId) {
		Stream<T> stream = ConcurrentLinkedListStreamFactory.INSTANCE.newStream(fromId, toId);
		String inRateFile = getFileName(folder, stream.getId(), inRateExension);
		String outRateFile = getFileName(folder, stream.getId(), outRateExtension);
		return new StreamStatistic<>(stream, inRateFile, outRateFile, autoFlush);
	}

	private static String getFileName(String folder, String baseName, String extension) {
		return folder + File.separator + baseName + "." + extension + ".csv";
	}
}
