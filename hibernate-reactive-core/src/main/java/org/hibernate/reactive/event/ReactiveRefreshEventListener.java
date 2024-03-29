/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.event.spi.RefreshContext;
import org.hibernate.event.spi.RefreshEvent;

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
	CompletionStage<Void> reactiveOnRefresh(RefreshEvent event);

	CompletionStage<Void> reactiveOnRefresh(RefreshEvent event, RefreshContext refreshedAlready);

}
