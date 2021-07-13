/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.engine.internal.TwoPhaseLoad;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.PreLoadEvent;
import org.hibernate.loader.plan.exec.query.spi.NamedParameterContext;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.EntityType;
import org.hibernate.type.OneToOneType;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

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
								? resolve( entityType, (Serializable) value, owner, source )
								: entityType.resolve( value, source, owner, overridingEager )
		);

		final Object[] hydratedState = entityEntry.getLoadedState();
		CompletionStage<Void> loop = CompletionStages.voidFuture();
		for ( int i = 0; i < hydratedState.length; i++ ) {
			final Object o = hydratedState[i];
			if ( o instanceof CompletionStage ) {
				final CompletionStage<?> c = (CompletionStage) o;
				final int currentIndex = i;
				loop = loop.thenCompose( v -> c.thenAccept( initializedEntity -> hydratedState[ currentIndex ] = initializedEntity ) );
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

	/**
	 * Replacement for {@link EntityType#resolve(Object, SharedSessionContractImplementor, Object, Boolean)}
	 */
	default CompletionStage<Object> resolve(EntityType entityType, Serializable value, Object owner,
											SharedSessionContractImplementor session) {
		if ( value != null && !isNull( entityType, owner, session ) ) {
			if ( entityType.isReferenceToPrimaryKey() ) {
				return ((ReactiveQueryExecutor) session).reactiveInternalLoad(
						entityType.getAssociatedEntityName(),
						value,
						true,
						entityType.isNullable()
				);
			}
			else {
				return loadByUniqueKey( entityType, value, session );
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

	/**
	 * Load an instance by a unique key that is not the primary key.
	 *
	 * @param entityType The {@link EntityType} of the association
	 * @param key The unique key property value.
	 * @param session The originating session.
	 *
	 * @return The loaded entity
	 *
	 * @throws HibernateException generally indicates problems performing the load.
	 */
	default CompletionStage<Object> loadByUniqueKey(
			EntityType entityType,
			Object key,
			SharedSessionContractImplementor session) throws HibernateException {
		SessionFactoryImplementor factory = session.getFactory();
		String entityName = entityType.getAssociatedEntityName();
		String uniqueKeyPropertyName = entityType.getRHSUniqueKeyPropertyName();

		ReactiveEntityPersister persister =
				(ReactiveEntityPersister) factory.getMetamodel().entityPersister( entityName );

		//TODO: implement 2nd level caching?! natural id caching ?! proxies?!

		EntityUniqueKey euk = new EntityUniqueKey(
				entityName,
				uniqueKeyPropertyName,
				key,
				entityType.getIdentifierOrUniqueKeyType( factory ),
				persister.getEntityMode(),
				factory
		);

		PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		Object result = persistenceContext.getEntity( euk );
		if ( result != null ) {
			return completedFuture( persistenceContext.proxyFor( result ) );
		}
		else {
			return persister.reactiveLoadByUniqueKey( uniqueKeyPropertyName, key, session )
					.thenApply( loaded -> {
						// If the entity was not in the Persistence Context, but was found now,
						// add it to the Persistence Context
						if ( loaded != null ) {
							persistenceContext.addEntity(euk, loaded);
						}
						return loaded;
					} );

		}
	}
}
