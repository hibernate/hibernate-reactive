/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.spi;


import org.hibernate.HibernateException;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.internal.util.collections.IdentitySet;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of deletion events generated from a session.
 *
 * @author Steve Ebersole
 */
public interface ReactiveDeleteEventListener extends Serializable {

	/**
	 * Handle the given delete event.
	 *
	 * @param event The delete event to be handled.
	 */
	CompletionStage<Void> reactiveOnDelete(DeleteEvent event) throws HibernateException;

	CompletionStage<Void> reactiveOnDelete(DeleteEvent event, IdentitySet transientEntities) throws HibernateException;
}
