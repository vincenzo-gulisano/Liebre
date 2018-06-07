package stream.smq.resource;

import common.component.Component;
import common.component.EventType;

public class NotifyingResourceManager extends AbstractResourceManager {

  private final Component component;
  private final EventType eventType;

  public NotifyingResourceManager(int size, Component component, EventType eventType) {
    super(size);
    this.component = component;
    this.eventType = eventType;
  }

  @Override
  protected void doAcquire() throws InterruptedException {
    component.wait(eventType);
  }

  @Override
  protected void doRelease() {
    component.notify(eventType);
  }
}
