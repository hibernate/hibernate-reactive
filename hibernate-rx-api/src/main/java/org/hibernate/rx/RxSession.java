package org.hibernate.rx;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Similar to the Hibernate session but the operations are reactive
 */
public interface RxSession {

	<T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id);

	CompletionStage<Void> persist(Object entity);

	CompletionStage<Void> flush();

	CompletionStage<Void> remove(Object entity);

	<R> RxQuery<R> createQuery(Class<R> resultType, String jpql);

	StateControl sessionState();
}
