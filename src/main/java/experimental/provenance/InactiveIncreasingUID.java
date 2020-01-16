package experimental.provenance;

public class InactiveIncreasingUID implements IncreasingUID {

  @Override
  public String increaseAndGet() {
    return null;
  }

  @Override
  public void update() {}
}
