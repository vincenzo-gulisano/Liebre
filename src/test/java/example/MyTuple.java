package example;

class MyTuple {

  public long timestamp;
  public int key;
  public int value;

  public MyTuple(long timestamp, int key, int value) {
    this.timestamp = timestamp;
    this.key = key;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getKey() {
    return key;
  }

  public void setKey(int key) {
    this.key = key;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
