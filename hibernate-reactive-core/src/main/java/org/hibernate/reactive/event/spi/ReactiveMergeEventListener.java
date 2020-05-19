/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.event.spi;

import org.hibernate.HibernateException;
import org.hibernate.event.internal.MergeContext;
import org.hibernate.event.spi.MergeEvent;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

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
	CompletionStage<Void> reactiveOnMerge(MergeEvent event) throws HibernateException;

    /** 
     * Handle the given merge event.
     *
     * @param event The merge event to be handled.
     */
	CompletionStage<Void> reactiveOnMerge(MergeEvent event, MergeContext copiedAlready) throws HibernateException;

}
