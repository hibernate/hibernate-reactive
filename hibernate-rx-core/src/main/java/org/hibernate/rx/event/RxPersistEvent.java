package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxSession;

/**
 * A {@link PersistEvent} for the reactive session
 */
public class RxPersistEvent extends PersistEvent {

	private CompletionStage<?> stage;

	public RxPersistEvent(
			String entityName,
			Object original,
			RxHibernateSession source,
			RxSession rxSession) {
		super( entityName, original, (EventSource) source );
	}

	public void setStage(CompletionStage<?> stage) {
		this.stage = stage;
	}

	public CompletionStage<?> getStage() {
		return stage;
	}
}