package experimental.provenance;

import common.tuple.BaseRichTuple;
import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

//TODO: Custom serialization
public class GenealogTuple extends BaseRichTuple implements Serializable {

	private String UID;
	public transient GenealogTuple U1, U2, N;
	public GenealogTupleType type = GenealogTupleType.SOURCE;

	public GenealogTuple(long stimulus, long timestamp, String key) {
		super(stimulus, timestamp, key);
	}

	public String getUID() {
		return UID;
	}

	public void setUID(String UID) {
		if (this.UID != null) {
			throw new IllegalStateException("Tuple UID can only be set once!");
		}
		this.UID = UID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		GenealogTuple genealogTuple = (GenealogTuple) o;
		return Objects.equals(UID, genealogTuple.UID) &&
				type == genealogTuple.type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), UID, type);
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this)
				.append("UID", UID)
				.append("type", type)
        .appendSuper(super.toString())
				.toString();
	}
}
