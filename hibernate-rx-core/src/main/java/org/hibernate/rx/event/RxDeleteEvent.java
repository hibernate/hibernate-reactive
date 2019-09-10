package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.rx.RxSession;

/**
 * A {@link org.hibernate.event.spi.DeleteEvent} for the reactive session
 */
public class RxDeleteEvent extends DeleteEvent {

	private final CompletionStage<Void> stage;

	public RxDeleteEvent(
			String entityName,
			Object original,
			EventSource source,
			RxSession rxSession,
			CompletionStage<Void> stage) {
		super( entityName, original, source );
		this.stage = stage;
	}

	public CompletionStage<Void> getStage() {
		return stage;
	}
}