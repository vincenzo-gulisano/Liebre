package experimental.provenance;


import common.tuple.BaseRichTuple;
import java.io.Serializable;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class GenealogRemoteTuple extends BaseRichTuple implements Serializable {

  private final GenealogTuple tuple;
  private final String provenanceUID;


  public GenealogRemoteTuple(GenealogTuple tuple, long timestamp, String provenanceUID) {
    super(tuple.getStimulus(), timestamp, tuple.getKey());
    this.tuple = tuple;
    this.provenanceUID = provenanceUID;
  }

  public GenealogTuple getTuple() {
    return tuple;
  }

  public String getProvenanceUID() {
    return provenanceUID;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("tuple", tuple)
        .append("provenanceUID", provenanceUID)
        .appendSuper(super.toString())
        .toString();
  }
}
