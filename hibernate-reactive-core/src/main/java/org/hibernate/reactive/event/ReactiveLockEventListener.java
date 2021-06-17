/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.LockEvent;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of lock events generated from a session.
 *
 * @author Steve Ebersole
 */
public interface ReactiveLockEventListener extends Serializable {

    /**
	 * Handle the given lock event.
     *
     * @param event The lock event to be handled.
     */
	CompletionStage<Void> reactiveOnLock(LockEvent event) throws HibernateException;
}
