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
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.EntityType;
import org.hibernate.type.OneToOneType;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

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
				(entityType, value, source, owner, overridingEager)
						-> entityType.isEager( overridingEager )
								? resolve( entityType, (Serializable) value, owner, source )
								: entityType.resolve(value, source, owner, overridingEager)
		);

		final Object[] hydratedState = entityEntry.getLoadedState();
		return CompletionStages.loopWithoutTrampoline(
				IntStream.range( 0, hydratedState.length )
						.filter( i-> hydratedState[ i ] instanceof CompletionStage ),
				i -> ( (CompletionStage<Object>) hydratedState[ i ] )
						.thenAccept( initializedEntity -> hydratedState[ i ] = initializedEntity )
		)
		.thenAccept(
				v -> TwoPhaseLoad.initializeEntityFromEntityEntryLoadedState(
						entity,
						entityEntry,
						readOnly,
						session,
						preLoadEvent,
						listeners
				)
		);
	}

	/**
	 * Replacement for {@link EntityType#resolve(Object, SharedSessionContractImplementor, Object, Boolean)}
	 */
	default CompletionStage<Object> resolve(EntityType entityType, Serializable value, Object owner,
											SharedSessionContractImplementor session) {
		if ( value != null && !isNull( entityType, owner, session ) ) {
			if ( entityType.isReferenceToPrimaryKey() ) {
				return ((ReactiveSession) session).reactiveInternalLoad(
						entityType.getAssociatedEntityName(),
						value,
						true,
						entityType.isNullable()
				);
			}
			else {
				//TODO see EntityType.resolve() we need to reactify loadByUniqueKey()
				throw new UnsupportedOperationException("unique key property name not supported here");
			}
		}
		else {
			return null;
		}
	}

	default boolean isNull(EntityType entityType, Object owner,
						   SharedSessionContractImplementor session) {
		if ( entityType instanceof OneToOneType ) {
			OneToOneType type = (OneToOneType) entityType;
			String propertyName = type.getPropertyName();
			if ( propertyName != null ) {
				EntityPersister ownerPersister =
						session.getFactory().getMetamodel()
								.entityPersister( entityType.getAssociatedEntityName() );
				Serializable id = session.getContextEntityIdentifier(owner);
				EntityKey entityKey = session.generateEntityKey(id, ownerPersister);
				return session.getPersistenceContextInternal().isPropertyNull( entityKey, propertyName);
			}
			else {
				return false;
			}
		}
		else {
			return false;
		}
	}
}
