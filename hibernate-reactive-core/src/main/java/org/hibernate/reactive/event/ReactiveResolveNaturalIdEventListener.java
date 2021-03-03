/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.ResolveNaturalIdEvent;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of resolve natural id events generated from a session.
 *
 * @author Eric Dalquist
 * @author Steve Ebersole
 */
public interface ReactiveResolveNaturalIdEventListener extends Serializable {

	/**
	 * Handle the given resolve natural id event.
	 *
	 * @param event The resolve natural id event to be handled.
	 *
	 * @throws HibernateException Indicates a problem resolving natural id to primary key
	 */
	public CompletionStage<Void> reactiveResolveNaturalId(ResolveNaturalIdEvent event) throws HibernateException;

}
