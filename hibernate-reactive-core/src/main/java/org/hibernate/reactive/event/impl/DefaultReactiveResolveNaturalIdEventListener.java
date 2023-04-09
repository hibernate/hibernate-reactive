/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.NaturalIdResolutions;
import org.hibernate.event.internal.AbstractLockUpgradeEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.ResolveNaturalIdEvent;
import org.hibernate.event.spi.ResolveNaturalIdEventListener;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.event.ReactiveResolveNaturalIdEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultResolveNaturalIdEventListener}.
 */
public class DefaultReactiveResolveNaturalIdEventListener extends AbstractLockUpgradeEventListener
		implements ReactiveResolveNaturalIdEventListener, ResolveNaturalIdEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public void onResolveNaturalId(ResolveNaturalIdEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> onReactiveResolveNaturalId(ResolveNaturalIdEvent event) throws HibernateException {
		return resolveNaturalId( event ).thenAccept( event::setEntityId );
	}

	/**
	 * Coordinates the efforts to load a given entity. First, an attempt is
	 * made to load the entity from the session-level cache. If not found there,
	 * an attempt is made to locate it in second-level cache. Lastly, an
	 * attempt is made to load it directly from the datasource.
	 *
	 * @param event The load event
	 *
	 * @return The loaded entity, or null.
	 */
	protected CompletionStage<Object> resolveNaturalId(ResolveNaturalIdEvent event) {
		final EntityPersister persister = event.getEntityPersister();

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Attempting to resolve: {0}#{1}",
					infoString( persister ),
					event.getNaturalIdValues()
			);
		}

		final Object entityId = resolveFromCache( event );
		if ( entityId != null ) {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev(
						"Resolved object in cache: {0}#{1}",
						infoString( persister ),
						event.getNaturalIdValues() );
			}
			return completedFuture( entityId );
		}

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Object not resolved in any cache: {0}#{1}",
					infoString( persister ),
					event.getNaturalIdValues()
			);
		}

		return loadFromDatasource( event );
	}

	/**
	 * Attempts to resolve the entity id corresponding to the event's natural id values from the session
	 *
	 * @param event The load event
	 * @return The entity from the cache, or null.
	 */
	protected Object resolveFromCache(ResolveNaturalIdEvent event) {
		return getNaturalIdResolutions( event )
				.findCachedIdByNaturalId( event.getOrderedNaturalIdValues(), event.getEntityPersister() );
	}

	/**
	 * Performs the process of loading an entity from the configured
	 * underlying datasource.
	 *
	 * @param event The load event
	 *
	 * @return The object loaded from the datasource, or null if not found.
	 */
	protected CompletionStage<Object> loadFromDatasource(ResolveNaturalIdEvent event) {
		final EventSource session = event.getSession();
		final EntityPersister entityPersister = event.getEntityPersister();
		final StatisticsImplementor statistics = session.getFactory().getStatistics();
		final boolean statisticsEnabled = statistics.isStatisticsEnabled();
		final long startTime = statisticsEnabled ? System.nanoTime() : 0;

		return ( (ReactiveEntityPersister) entityPersister)
				.reactiveLoadEntityIdByNaturalId( event.getOrderedNaturalIdValues(), event.getLockOptions(), session )
				.thenApply( pk -> {
					if ( statisticsEnabled ) {
						long milliseconds = MILLISECONDS.convert( System.nanoTime() - startTime, NANOSECONDS );
						statistics.naturalIdQueryExecuted( entityPersister.getRootEntityName(), milliseconds );
					}

					//PK can be null if the entity doesn't exist
					if ( pk != null ) {
						getNaturalIdResolutions( event )
								.cacheResolutionFromLoad( pk, event.getOrderedNaturalIdValues(), entityPersister );
					}

					return pk;
				} );
	}

	private static NaturalIdResolutions getNaturalIdResolutions(ResolveNaturalIdEvent event) {
		return event.getSession().getPersistenceContextInternal().getNaturalIdResolutions();
	}
}
