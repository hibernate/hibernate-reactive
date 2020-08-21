/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader;

import org.hibernate.AssertionFailure;
import org.hibernate.engine.internal.TwoPhaseLoad;
import org.hibernate.engine.spi.*;
import org.hibernate.event.spi.PreLoadEvent;
import org.hibernate.event.spi.PreLoadEventListener;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.loader.plan.exec.query.spi.NamedParameterContext;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * An interface intended to unify how a ResultSet is processed by
 * by {@link ReactiveLoader} implementations..
 */
public interface ReactiveResultSetProcessor {

	CoreMessageLogger LOG = CoreLogging.messageLogger(ReactiveLoaderBasedResultSetProcessor.class);

	CompletionStage<List<Object>> reactiveExtractResults(
			ResultSet resultSet,
			final SharedSessionContractImplementor session,
			QueryParameters queryParameters,
			NamedParameterContext namedParameterContext,
			boolean returnProxies,
			boolean readOnly,
			ResultTransformer forcedResultTransformer,
			List<AfterLoadAction> afterLoadActionList) throws SQLException;

	@SuppressWarnings("unchecked")
	default CompletionStage<Void> initializeEntity(
			final Object entity,
			final boolean readOnly,
			final SharedSessionContractImplementor session,
			final PreLoadEvent preLoadEvent,
			Iterable<PreLoadEventListener> listeners) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
		final EntityEntry entityEntry = persistenceContext.getEntry(entity);
		if (entityEntry == null) {
			throw new AssertionFailure("possible non-threadsafe access to the session");
		}

		TwoPhaseLoad.initializeEntityEntryLoadedState(
				entity,
				entityEntry,
				session,
				(entityType, value, session1, owner, overridingEager)
						-> entityType.isEager( overridingEager )
								? ((ReactiveSession) session1).reactiveGet( entityType.getReturnedClass(), (Serializable) value )
								: entityType.resolve(value, session1, owner, overridingEager)
		);

		CompletionStage<Void> stage = CompletionStages.voidFuture();

		final Object[] hydratedState = entityEntry.getLoadedState();
		for ( int i = 0 ; i < hydratedState.length ; i++ ) {
			if ( hydratedState[ i ] instanceof CompletionStage ) {
				final int iConstant = i;
				stage = stage.thenCompose( v -> (CompletionStage<Object>) hydratedState[ iConstant ] )
						.thenAccept( initializedEntity -> hydratedState[ iConstant ] = initializedEntity );
			}
		}

		return stage.thenAccept( v -> TwoPhaseLoad.initializeEntityFromEntityEntryLoadedState(
				entity,
				entityEntry,
				readOnly,
				session,
				preLoadEvent,
				listeners
		) );
	}
}
