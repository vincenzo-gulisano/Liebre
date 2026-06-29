package experimental.provenance;

import component.operator.Operator;

/**
 * Optional extension point for operators that can build their own provenance-safe replacement.
 *
 * <p>{@link ProvenanceQueryTransformer} uses this interface when it finds an operator that is not
 * handled by the built-in Liebre provenance rules. Implementing this interface lets custom
 * operators define their own provenance behavior without modifying the transformer.
 *
 * <p>The implementation must return a fresh, disconnected operator instance. It must not mutate the
 * original operator, must not return {@code this}, and must not copy any already-connected input or
 * output streams from the original query. The query transformer is responsible for reconnecting the
 * returned operator using the original query graph metadata.
 *
 * <p>The replacement must preserve the original operator's input shape. If the original operator is
 * an {@code Operator2In}, the returned operator must also be an {@code Operator2In}. For one-input
 * operators, the returned operator must be a one-input operator. This is required so that the
 * transformer can replay the original connections correctly.
 *
 * <p>Operators that forward existing {@link GenealogTuple}s should normally keep the tuples
 * unchanged. Operators that create new tuples should use the provided {@link
 * ProvenanceTransformationContext} to assign UID, type, and parent links consistently with the rest
 * of the provenance package.
 */
public interface ProvenanceTransformableOperator {

  /**
   * Create a provenance-safe replacement for this operator.
   *
   * @param context Helper for assigning GeneaLog provenance metadata consistently.
   * @return A fresh, disconnected operator to insert in the transformed query.
   */
  Operator<?, ?> createProvenanceOperator(ProvenanceTransformationContext context);
}
