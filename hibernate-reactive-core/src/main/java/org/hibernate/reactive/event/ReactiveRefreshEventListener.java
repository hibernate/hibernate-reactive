/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.RefreshEvent;
import org.hibernate.internal.util.collections.IdentitySet;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of refresh events generated from a session.
 *
 * @author Steve Ebersole
 */
public interface ReactiveRefreshEventListener extends Serializable {

    /**
     * Handle the given refresh event.
     *
     * @param event The refresh event to be handled.
     */
	CompletionStage<Void> reactiveOnRefresh(RefreshEvent event) throws HibernateException;

	CompletionStage<Void> reactiveOnRefresh(RefreshEvent event, IdentitySet refreshedAlready) throws HibernateException;

}
