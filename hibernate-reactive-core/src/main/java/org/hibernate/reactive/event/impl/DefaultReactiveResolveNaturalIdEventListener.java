/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.event.internal.AbstractLockUpgradeEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.ResolveNaturalIdEvent;
import org.hibernate.event.spi.ResolveNaturalIdEventListener;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.event.ReactiveResolveNaturalIdEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultResolveNaturalIdEventListener}.
 */
public class DefaultReactiveResolveNaturalIdEventListener
		extends AbstractLockUpgradeEventListener
		implements ReactiveResolveNaturalIdEventListener, ResolveNaturalIdEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public void onResolveNaturalId(ResolveNaturalIdEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> reactiveResolveNaturalId(ResolveNaturalIdEvent event) throws HibernateException {
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
	protected CompletionStage<Serializable> resolveNaturalId(ResolveNaturalIdEvent event) {
		EntityPersister persister = event.getEntityPersister();

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Attempting to resolve: {0}#{1}",
					MessageHelper.infoString( persister ),
					event.getNaturalIdValues()
			);
		}

		Serializable entityId = resolveFromCache( event );
		if ( entityId != null ) {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev(
						"Resolved object in cache: {0}#{1}",
						MessageHelper.infoString( persister ),
						event.getNaturalIdValues()
				);
			}
			return completedFuture( entityId );
		}

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Object not resolved in any cache: {0}#{1}",
					MessageHelper.infoString( persister ),
					event.getNaturalIdValues()
			);
		}

		return loadFromDatasource( event );
	}

	/**
	 * Attempts to resolve the entity id corresponding to the event's natural id values from the session
	 *
	 * @param event The load event
	 *
	 * @return The entity from the cache, or null.
	 */
	protected Serializable resolveFromCache(ResolveNaturalIdEvent event) {
		return getNaturalIdHelper( event )
				.findCachedNaturalIdResolution( event.getEntityPersister(), event.getOrderedNaturalIdValues() );
	}

	/**
	 * Performs the process of loading an entity from the configured
	 * underlying datasource.
	 *
	 * @param event The load event
	 *
	 * @return The object loaded from the datasource, or null if not found.
	 */
	protected CompletionStage<Serializable> loadFromDatasource(ResolveNaturalIdEvent event) {
		EventSource session = event.getSession();
		StatisticsImplementor statistics = session.getFactory().getStatistics();
		boolean statisticsEnabled = statistics.isStatisticsEnabled();
		long startTime = statisticsEnabled ? System.nanoTime() : 0;
		EntityPersister entityPersister = event.getEntityPersister();
		Object[] orderedNaturalIdValues = event.getOrderedNaturalIdValues();
		return ( (ReactiveEntityPersister) entityPersister)
				.reactiveLoadEntityIdByNaturalId( orderedNaturalIdValues, event.getLockOptions(), session )
				.thenApply( pk -> {
					if (statisticsEnabled) {
						statistics.naturalIdQueryExecuted(
								entityPersister.getRootEntityName(),
								MILLISECONDS.convert( System.nanoTime() - startTime, NANOSECONDS )
						);
					}

					//PK can be null if the entity doesn't exist
					if (pk != null) {
						getNaturalIdHelper( event )
								.cacheNaturalIdCrossReferenceFromLoad( entityPersister, pk, orderedNaturalIdValues );
					}

					return pk;
				} );
	}

	private static PersistenceContext.NaturalIdHelper getNaturalIdHelper(ResolveNaturalIdEvent event) {
		return event.getSession().getPersistenceContextInternal().getNaturalIdHelper();
	}
}
