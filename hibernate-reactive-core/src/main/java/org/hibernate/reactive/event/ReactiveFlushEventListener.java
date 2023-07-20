/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.FlushEvent;

import java.io.Serializable;
import org.hibernate.reactive.engine.impl.InternalStage;

/**
 * Defines the contract for handling of reactive session flush events.
 */
public interface ReactiveFlushEventListener extends Serializable {
	/**
	 * Handle the given flush event.
	 *
	 * @param event The flush event to be handled.
	 */
	InternalStage<Void> reactiveOnFlush(FlushEvent event) throws HibernateException;
}
