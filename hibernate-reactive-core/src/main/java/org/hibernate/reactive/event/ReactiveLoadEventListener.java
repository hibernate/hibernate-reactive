/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;


import java.io.Serializable;
import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener.LoadType;

/**
 * Defines the contract for handling of load events generated from a session.
 *
 * @author Steve Ebersole
 */
public interface ReactiveLoadEventListener extends Serializable {

	/**
	 * Handle the given load event.
	 *
	 * @param event The load event to be handled.
	 *
	 * @throws HibernateException
	 */
	InternalStage<Void> reactiveOnLoad(LoadEvent event, LoadType loadType) throws HibernateException;

}
