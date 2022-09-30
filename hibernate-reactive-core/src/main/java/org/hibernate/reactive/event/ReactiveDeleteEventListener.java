/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;


import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.DeleteEvent;

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

	CompletionStage<Void> reactiveOnDelete(DeleteEvent event, DeleteContext transientEntities) throws HibernateException;
}
