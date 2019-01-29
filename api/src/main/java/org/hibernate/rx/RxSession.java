package org.hibernate.rx;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Right now we are playing around, but this is going to be the core
 * interface of the project.
 */
public interface RxSession {

	<T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id);

	CompletionStage<Void> persist(Object entity);

	CompletionStage<Void> remove(Object entity);

	<R> RxQuery<R> createQuery(Class<R> resultType, String jpql);

	StateControl sessionState();

}
