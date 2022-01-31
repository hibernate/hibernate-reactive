/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.engine.internal.TwoPhaseLoad;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.PreLoadEvent;
import org.hibernate.loader.plan.exec.query.spi.NamedParameterContext;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.engine.impl.EntityTypes;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.transform.ResultTransformer;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * An interface intended to unify how a ResultSet is processed by
 * by {@link ReactiveLoader} implementations..
 */
public interface ReactiveResultSetProcessor {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	CompletionStage<List<Object>> reactiveExtractResults(
			ResultSet resultSet,
			final SharedSessionContractImplementor session,
			QueryParameters queryParameters,
			NamedParameterContext namedParameterContext,
			boolean returnProxies,
			boolean readOnly,
			ResultTransformer forcedResultTransformer,
			List<AfterLoadAction> afterLoadActionList) throws SQLException;

	default CompletionStage<Void> initializeEntity(
			final Object entity,
			final boolean readOnly,
			final SharedSessionContractImplementor session,
			final PreLoadEvent preLoadEvent) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
		final EntityEntry entityEntry = persistenceContext.getEntry(entity);
		if (entityEntry == null) {
			throw new AssertionFailure("possible non-threadsafe access to the session");
		}

		TwoPhaseLoad.initializeEntityEntryLoadedState(
				entity,
				entityEntry,
				session,
				(entityType, value, source, owner, overridingEager)
						-> entityType.isEager( overridingEager )
								? EntityTypes.resolve( entityType, value, owner, source )
								: entityType.resolve( value, source, owner, overridingEager )
		);

		final Object[] hydratedState = entityEntry.getLoadedState();
		CompletionStage<Void> loop = voidFuture();
		for ( int i = 0; i < hydratedState.length; i++ ) {
			final Object state = hydratedState[i];
			if ( state instanceof CompletionStage ) {
				final int currentIndex = i;
				loop = loop
						.thenCompose( v -> (CompletionStage<?>) state )
						.thenAccept( initializedEntity -> hydratedState[currentIndex] = initializedEntity );
			}
		}
		return loop.thenAccept(
				v -> TwoPhaseLoad.initializeEntityFromEntityEntryLoadedState(
						entity,
						entityEntry,
						readOnly,
						session,
						preLoadEvent
				)
		);
	}
}
