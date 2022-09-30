/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.PersistContext;
import org.hibernate.event.spi.PersistEvent;

/**
 * Defines the contract for handling of create events generated from a session.
 *
 * @author Gavin King
 */
public interface ReactivePersistEventListener {

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 */
	CompletionStage<Void> reactiveOnPersist(PersistEvent event);

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 */
	CompletionStage<Void> reactiveOnPersist(PersistEvent event, PersistContext createdAlready);

}
