package org.hibernate.rx;

import org.hibernate.Session;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.rx.engine.spi.RxActionQueue;

import javax.persistence.LockModeType;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * A Hibernate {@link Session} that allows the creation of a reactive session
 *
 *  @see RxSession
 */
public interface RxSessionInternal extends Session {

	@Override
	RxSessionFactory getSessionFactory();

	SessionImplementor delegate();

	RxSession reactive();

	// Alternative
	void reactive(Consumer<RxSession> consumer);

	RxActionQueue getRxActionQueue();

	<T> CompletionStage<Optional<T>> rxFetch(T association);

	CompletionStage<Void> rxPersist(Object entity);

	CompletionStage<Void> rxRemove(Object entity);

	CompletionStage<Void> rxFlush();

	<T> CompletionStage<Optional<T>> rxFind(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties);

}
