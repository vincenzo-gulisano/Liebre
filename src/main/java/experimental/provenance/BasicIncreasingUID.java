package experimental.provenance;

import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BasicIncreasingUID implements IncreasingUID {

  private final String UID = UUID.randomUUID().toString();
  private int counter;

  @Override
  public String increaseAndGet() {
    update();
    return UID + counter;
  }

  @Override
  public void update() {
    counter += 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BasicIncreasingUID uid = (BasicIncreasingUID) o;
    return counter == uid.counter && Objects.equals(UID, uid.UID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(UID, counter);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("UID", UID)
        .append("counter", counter)
        .toString();
  }
}
