package common.component;

public enum ConnectionsNumber {
  NONE {
    @Override
    protected boolean isValid(int number) {
      return number == 0;
    }
  },
  ONE {
    @Override
    protected boolean isValid(int number) {
      return number == 1;
    }
  },
  TWO {
    @Override
    protected boolean isValid(int number) {
      return number == 2;
    }
  },
  N {
    @Override
    protected boolean isValid(int number) {
      return number >= 1;
    }
  };

  protected abstract boolean isValid(int number);

  public boolean isMultiple() {
    return this == TWO || this == N;
  }
}
