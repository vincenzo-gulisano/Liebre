package experimental.provenance;

import component.operator.Operator;

/**
 * External provenance transformation for an operator class.
 *
 * <p>This interface is useful when the operator class cannot implement {@link
 * ProvenanceTransformableOperator} directly. Implementations must create a fresh, disconnected
 * operator that is safe to use in the transformed query. The returned operator must not be the
 * original instance.
 *
 * @param <T> The original operator type handled by this transformer.
 */
public interface ProvenanceOperatorTransformer<T extends Operator<?, ?>> {

  /**
   * Create a provenance-safe replacement for {@code operator}.
   *
   * <p>The replacement must preserve the input arity of the original operator. In particular, if
   * {@code operator} is an {@code Operator2In}, the returned operator must also be an {@code
   * Operator2In}, otherwise the transformed query cannot reconnect the original graph.
   *
   * @param operator The original operator from the query being transformed.
   * @param context Helper for assigning GeneaLog provenance metadata consistently.
   * @return A fresh, disconnected operator to insert in the transformed query.
   */
  Operator<?, ?> transform(T operator, ProvenanceTransformationContext context);
}
