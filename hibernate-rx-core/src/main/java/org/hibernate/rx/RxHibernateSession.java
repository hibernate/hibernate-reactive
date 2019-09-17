package org.hibernate.rx;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.Session;
import org.hibernate.rx.engine.spi.RxActionQueue;

/**
 * A Hibernate {@link Session} that allows the creation of a reactive session
 *
 *  @see RxSession
 */
public interface RxHibernateSession extends Session {

	@Override
	RxHibernateSessionFactory getSessionFactory();

	RxSession reactive();

	// Alternative
	void reactive(Consumer<RxSession> consumer);

	RxActionQueue getRxActionQueue();

}
