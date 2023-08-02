/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import java.io.Serializable;
import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.MergeContext;
import org.hibernate.event.spi.MergeEvent;

/**
 * Defines the contract for handling of merge events generated from a session.
 *
 * @author Gavin King
 */
public interface ReactiveMergeEventListener extends Serializable {

    /**
     * Handle the given merge event.
     *
     * @param event The merge event to be handled.
     */
	InternalStage<Void> reactiveOnMerge(MergeEvent event) throws HibernateException;

    /**
     * Handle the given merge event.
     *
     * @param event The merge event to be handled.
     */
	InternalStage<Void> reactiveOnMerge(MergeEvent event, MergeContext copiedAlready) throws HibernateException;

}
