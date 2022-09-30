/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.type.EntityType;
import org.hibernate.type.ForeignKeyDirection;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer.UNFETCHED_PROPERTY;
import static org.hibernate.property.access.internal.PropertyAccessStrategyBackRefImpl.UNKNOWN;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;

/**
 * Reactive operations that really belong to {@link EntityType}
 *
 * @author Gavin King
 */
public class EntityTypes {

	/**
	 * Replacement for {@link EntityType#resolve(Object, SharedSessionContractImplementor, Object, Boolean)}
	 */
	public static CompletionStage<Object> resolve(EntityType entityType, Object idOrUniqueKey, Object owner,
			SharedSessionContractImplementor session) {
		if ( idOrUniqueKey != null && !isNull( entityType, owner, session ) ) {
			if ( entityType.isReferenceToPrimaryKey() ) {
				return ReactiveQueryExecutorLookup.extract( session ).reactiveInternalLoad(
						entityType.getAssociatedEntityName(),
						(Serializable) idOrUniqueKey,
						true,
						entityType.isNullable()
				);
			}
			else {
				return loadByUniqueKey( entityType, idOrUniqueKey, session );
			}
		}
		else {
			return null;
		}
	}

	/**
	 * @see OneToOneType#isNull(Object, SharedSessionContractImplementor)
	 */
	static boolean isNull(EntityType entityType, Object owner, SharedSessionContractImplementor session) {
		if ( entityType instanceof OneToOneType ) {
			OneToOneType type = (OneToOneType) entityType;
			String propertyName = type.getPropertyName();
			if ( propertyName != null ) {
				EntityPersister ownerPersister = session.getFactory().getMetamodel()
						.entityPersister( entityType.getAssociatedEntityName() );
				Object id = session.getContextEntityIdentifier( owner );
				EntityKey entityKey = session.generateEntityKey( id, ownerPersister );
				return session.getPersistenceContextInternal().isPropertyNull( entityKey, propertyName );
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
	 * @return The loaded entity
	 * @throws HibernateException generally indicates problems performing the load.
	 */
	static CompletionStage<Object> loadByUniqueKey(
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
							persistenceContext.addEntity( euk, loaded );
						}
						return loaded;
					} );

		}
	}

	/**
	 * @see org.hibernate.type.TypeHelper#replace(Object[], Object[], Type[], SharedSessionContractImplementor, Object, Map)
	 */
	public static CompletionStage<Object[]> replace(
			final Object[] original,
			final Object[] target,
			final Type[] types,
			final SessionImplementor session,
			final Object owner,
			final Map copyCache) {
		Object[] copied = new Object[original.length];
		for ( int i = 0; i < types.length; i++ ) {
			if ( original[i] == UNFETCHED_PROPERTY || original[i] == UNKNOWN ) {
				copied[i] = target[i];
			}
			else {
				if ( !( types[i] instanceof EntityType ) ) {
					copied[i] = types[i].replace(
							original[i],
							target[i] == UNFETCHED_PROPERTY ? null : target[i],
							session,
							owner,
							copyCache
					);
				}
			}
		}
		return loop( 0, types.length,
					 i -> original[i] != UNFETCHED_PROPERTY && original[i] != UNKNOWN
							 && types[i] instanceof EntityType,
					 i -> replace(
							 (EntityType) types[i],
							 original[i],
							 target[i] == UNFETCHED_PROPERTY ? null : target[i],
							 session,
							 owner,
							 copyCache
					 ).thenAccept( copy -> copied[i] = copy )
		).thenApply( v -> copied );
	}

	/**
	 * @see org.hibernate.type.TypeHelper#replace(Object[], Object[], Type[], SharedSessionContractImplementor, Object, Map, ForeignKeyDirection)
	 */
	public static CompletionStage<Object[]> replace(
			final Object[] original,
			final Object[] target,
			final Type[] types,
			final SessionImplementor session,
			final Object owner,
			final Map copyCache,
			final ForeignKeyDirection foreignKeyDirection) {
		Object[] copied = new Object[original.length];
		for ( int i = 0; i < types.length; i++ ) {
			if ( original[i] == UNFETCHED_PROPERTY || original[i] == UNKNOWN ) {
				copied[i] = target[i];
			}
			else {
				if ( !( types[i] instanceof EntityType ) ) {
					copied[i] = types[i].replace(
							original[i],
							target[i] == UNFETCHED_PROPERTY ? null : target[i],
							session,
							owner,
							copyCache,
							foreignKeyDirection
					);
				}
			}
		}
		return loop( 0, types.length,
					 i -> original[i] != UNFETCHED_PROPERTY && original[i] != UNKNOWN
							 && types[i] instanceof EntityType,
					 i -> replace(
							 (EntityType) types[i],
							 original[i],
							 target[i] == UNFETCHED_PROPERTY ? null : target[i],
							 session,
							 owner,
							 copyCache,
							 foreignKeyDirection
					 ).thenAccept( copy -> copied[i] = copy )
		).thenApply( v -> copied );
	}

	/**
	 * @see org.hibernate.type.AbstractType#replace(Object, Object, SharedSessionContractImplementor, Object, Map, ForeignKeyDirection)
	 */
	private static CompletionStage<Object> replace(
			EntityType entityType,
			Object original,
			Object target,
			SessionImplementor session,
			Object owner,
			Map copyCache,
			ForeignKeyDirection foreignKeyDirection)
			throws HibernateException {
		boolean include = entityType.isAssociationType()
				? entityType.getForeignKeyDirection() == foreignKeyDirection
				: ForeignKeyDirection.FROM_PARENT == foreignKeyDirection;
		return include
				? replace( entityType, original, target, session, owner, copyCache )
				: completedFuture( target );
	}

	/**
	 * @see EntityType#replace(Object, Object, SharedSessionContractImplementor, Object, Map)
	 */
	private static CompletionStage<Object> replace(
			EntityType entityType,
			Object original,
			Object target,
			SessionImplementor session,
			Object owner,
			Map copyCache) {
		if ( original == null ) {
			return nullFuture();
		}
		Object cached = copyCache.get( original );
		if ( cached != null ) {
			return completedFuture( cached );
		}
		else {
			if ( original == target ) {
				return completedFuture( target );
			}
			if ( session.getContextEntityIdentifier( original ) == null ) {
				return ForeignKeys.isTransient( entityType.getAssociatedEntityName(), original, false, session )
						.thenCompose( isTransient -> {
							if ( isTransient ) {
								// original is transient; it is possible that original is a "managed" entity that has
								// not been made persistent yet, so check if copyCache contains original as a "managed" value
								// that corresponds with some "merge" value.
								if ( copyCache.containsValue( original ) ) {
									return completedFuture( original );
								}
								else {
									// the transient entity is not "managed"; add the merge/managed pair to copyCache
									final Object copy = session.getEntityPersister( entityType.getAssociatedEntityName(), original )
											.instantiate( null, session );
									copyCache.put( original, copy );
									return completedFuture( copy );
								}
							}
							else {
								return resolveIdOrUniqueKey( entityType, original, session, owner, copyCache );
							}
						} );
			}
			else {
				return resolveIdOrUniqueKey( entityType, original, session, owner, copyCache );
			}
		}
	}

	private static CompletionStage<Object> resolveIdOrUniqueKey(
			EntityType entityType,
			Object original,
			SessionImplementor session,
			Object owner,
			Map copyCache) {
		return getIdentifier( entityType, original, session )
				.thenCompose( id -> {
					if ( id == null ) {
						throw new AssertionFailure( "non-transient entity has a null id: " + original.getClass().getName() );
					}
					// For the special case of a @ManyToOne joined on a (non-primary) unique key,
					// the "id" class is actually the associated entity object itself, but treated
					// as a ComponentType. In the case that the entity is unfetched, we need to
					// explicitly fetch it here before calling replace(). (Note that in Hibernate
					// ORM this is unnecessary due to transparent lazy fetching.)
					return ( (ReactiveSessionImpl) session ).reactiveFetch( id, true )
							.thenCompose( fetched -> {
								Object idOrUniqueKey = entityType.getIdentifierOrUniqueKeyType( session.getFactory() )
										.replace( fetched, null, session, owner, copyCache );
								return resolve( entityType, idOrUniqueKey, owner, session );
							} );
				} );
	}

	/**
	 * see EntityType#getIdentifier(Object, SharedSessionContractImplementor)
	 */
	private static CompletionStage<Object> getIdentifier(EntityType entityType, Object value, SessionImplementor session) {
		if ( entityType.isReferenceToIdentifierProperty() ) {
			return ForeignKeys.getEntityIdentifierIfNotUnsaved(
					entityType.getAssociatedEntityName(),
					value,
					session ); // tolerates nulls
		}
		else if ( value == null ) {
			return nullFuture();
		}
		else {
			EntityPersister entityPersister = entityType.getAssociatedEntityPersister( session.getFactory() );
			String uniqueKeyPropertyName = entityType.getRHSUniqueKeyPropertyName();
			Object propertyValue = entityPersister.getPropertyValue( value, uniqueKeyPropertyName );
			// We now have the value of the property-ref we reference. However,
			// we need to dig a little deeper, as that property might also be
			// an entity type, in which case we need to resolve its identifier
			Type type = entityPersister.getPropertyType( uniqueKeyPropertyName );
			if ( type.isEntityType() ) {
				propertyValue = getIdentifier( (EntityType) type, propertyValue, session );
			}
			return completedFuture( propertyValue );
		}
	}

}
