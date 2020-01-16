package experimental.provenance;

import java.io.Serializable;

public interface IncreasingUID extends Serializable {

  String increaseAndGet();

  void update();

}
