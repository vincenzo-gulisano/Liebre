package experimental.provenance;

public enum UIDFactory {
  INSTANCE;
  private boolean enabledUIDs;

  public void enableUIDs() {
    System.out.println("[*] Enabling tuple UIDs...");
    this.enabledUIDs = true;
  }

  public IncreasingUID newUID() {
    return enabledUIDs ? new BasicIncreasingUID() : new InactiveIncreasingUID();
  }
}
