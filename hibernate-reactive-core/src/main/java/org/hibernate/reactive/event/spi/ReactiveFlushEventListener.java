/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.spi;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.FlushEvent;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of reactive session flush events.
 */
public interface ReactiveFlushEventListener extends Serializable {
	/**
	 * Handle the given flush event.
	 *
	 * @param event The flush event to be handled.
	 */
	CompletionStage<Void> reactiveOnFlush(FlushEvent event) throws HibernateException;
}
