/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.event.spi.ResolveNaturalIdEvent;

import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of resolve natural id events generated from a session.
 *
 * @author Eric Dalquist
 * @author Steve Ebersole
 *
 * @see org.hibernate.event.spi.ResolveNaturalIdEventListener
 */
public interface ReactiveResolveNaturalIdEventListener {

	/**
	 * Handle the given resolve natural id event.
	 *
	 * @param event The resolve natural id event to be handled.
	 */
	CompletionStage<Void> onReactiveResolveNaturalId(ResolveNaturalIdEvent event);

}
