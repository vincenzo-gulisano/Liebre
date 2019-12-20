package component;

import io.palyvos.dcs.common.Active;

public interface ComponentFunction extends Active {

  default boolean canRun() {
    return true;
  }

  @Override
  default void enable() {}

  @Override
  default boolean isEnabled() {
    return true;
  }

  @Override
  default void disable() {}
}
