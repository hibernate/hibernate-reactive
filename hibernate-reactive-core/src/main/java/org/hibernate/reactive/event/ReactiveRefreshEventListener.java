/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import org.hibernate.event.spi.RefreshContext;
import org.hibernate.event.spi.RefreshEvent;

import java.io.Serializable;
import org.hibernate.reactive.engine.impl.InternalStage;

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
	InternalStage<Void> reactiveOnRefresh(RefreshEvent event);

	InternalStage<Void> reactiveOnRefresh(RefreshEvent event, RefreshContext refreshedAlready);

}
