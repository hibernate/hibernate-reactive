package org.hibernate.rx;

import org.hibernate.Session;
import org.hibernate.rx.engine.spi.RxActionQueue;

import javax.persistence.LockModeType;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * A Hibernate {@link Session} that allows the creation of a reactive session
 *
 *  @see RxSession the actual user visible API
 */
public interface RxSessionInternal extends Session {

	RxSession reactive();

	RxActionQueue getRxActionQueue();

	<T> CompletionStage<Optional<T>> rxFetch(T association);

	CompletionStage<Void> rxPersist(Object entity);

	CompletionStage<Void> rxPersist(Object object, Map copiedAlready);

	CompletionStage<Void> rxPersistOnFlush(Object entity, Map copiedAlready);

	CompletionStage<Void> rxRemove(Object entity);

	CompletionStage<Void> rxRemove(Object entity, boolean isCascadeDeleteEnabled, Set transientObjects);

	CompletionStage<Void> rxFlush();

	CompletionStage<Void> rxRefresh(Object entity);

	CompletionStage<?> rxRefresh(Object child, Map refreshedAlready);

	<T> CompletionStage<Optional<T>> rxFind(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties);

}
