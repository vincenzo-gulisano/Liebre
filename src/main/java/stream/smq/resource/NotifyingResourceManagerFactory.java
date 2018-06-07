package stream.smq.resource;

import common.component.Component;
import common.component.EventType;

public class NotifyingResourceManagerFactory implements ResourceManagerFactory {

  private final Component component;
  private final EventType eventType;

  public NotifyingResourceManagerFactory(Component component, EventType eventType) {
    this.component = component;
    this.eventType = eventType;
  }


  @Override
  public ResourceManager newResourceManager(int size) {
    return new NotifyingResourceManager(size, component, eventType);
  }
}
