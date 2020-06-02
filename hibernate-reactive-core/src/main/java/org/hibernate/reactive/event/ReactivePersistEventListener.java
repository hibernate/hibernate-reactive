/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.internal.util.collections.IdentitySet;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of create events generated from a session.
 *
 * @author Gavin King
 */
public interface ReactivePersistEventListener extends Serializable {

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 *
	 * @throws HibernateException
	 */
	CompletionStage<Void> reactiveOnPersist(PersistEvent event) throws HibernateException;

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 *
	 * @throws HibernateException
	 */
	CompletionStage<Void> reactiveOnPersist(PersistEvent event, IdentitySet createdAlready) throws HibernateException;

}
