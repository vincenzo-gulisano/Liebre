package stream.smq.resource;

import common.component.Component;
import common.component.EventType;
import java.util.concurrent.atomic.AtomicInteger;

public class NotifyingResourceManager extends AbstractResourceManager {

  private final Component component;
  private final EventType eventType;
  private final AtomicInteger acquisitionCounter = new AtomicInteger(0);

  public NotifyingResourceManager(int size, Component component, EventType eventType) {
    super(size);
    this.component = component;
    this.eventType = eventType;
  }

  @Override
  protected void doAcquire() throws InterruptedException {
    if (acquisitionCounter.incrementAndGet() == size) {
      component.wait(eventType);
    }
  }

  @Override
  protected void doRelease() {
    acquisitionCounter.decrementAndGet();
    component.notify(eventType);
  }
}
