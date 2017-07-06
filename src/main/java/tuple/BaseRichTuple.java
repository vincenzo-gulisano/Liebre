package tuple;

public class BaseRichTuple implements RichTuple {

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
