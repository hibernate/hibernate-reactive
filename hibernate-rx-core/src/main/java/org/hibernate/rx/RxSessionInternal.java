package org.hibernate.rx;

import org.hibernate.Session;
import org.hibernate.rx.engine.spi.RxActionQueue;

import javax.persistence.LockModeType;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * A Hibernate {@link Session} backing the user-visible
 * {@link RxSession reactive session}. Note that even
 * though this interface extends {@code Session},
 * allowing it to be passed around through code in
 * Hibernate core, its non-reactive operations are
 * expected to throw {@link UnsupportedOperationException}.
 *
 *  @see RxSession the actual user visible API
 */
public interface RxSessionInternal extends Session {

	/**
	 * @return a reactive session backed by this object
	 */
	RxSession reactive();

	RxActionQueue getRxActionQueue();

	<T> CompletionStage<Optional<T>> rxFetch(T association);

	CompletionStage<Void> rxPersist(Object entity);

	CompletionStage<Void> rxPersist(Object object, Map copiedAlready);

	CompletionStage<Void> rxPersistOnFlush(Object entity, Map copiedAlready);

	CompletionStage<Void> rxRemove(Object entity);

	CompletionStage<Void> rxRemove(Object entity, boolean isCascadeDeleteEnabled, Set transientObjects);

	<T> CompletionStage<T> rxMerge(T object);

	CompletionStage<Void> rxMerge(Object object, Map copiedAlready);

	CompletionStage<Void> rxFlush();

	CompletionStage<Void> rxRefresh(Object entity);

	CompletionStage<?> rxRefresh(Object child, Map refreshedAlready);

	<T> CompletionStage<Optional<T>> rxGet(
			Class<T> entityClass,
			Serializable id);

	<T> CompletionStage<Optional<T>> rxFind(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties);

}
