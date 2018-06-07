package source;

@FunctionalInterface
public interface TextSourceFunction<T> {

  T getNext(String text);

}
