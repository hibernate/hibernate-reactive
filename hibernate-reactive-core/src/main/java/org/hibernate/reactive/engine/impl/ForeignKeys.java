/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.TransientObjectException;
import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.engine.internal.NonNullableTransientDependencies;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SelfDirtinessTracker;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.type.CompositeType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;

import static org.hibernate.engine.internal.ManagedTypeHelper.isHibernateProxy;
import static org.hibernate.engine.internal.ManagedTypeHelper.processIfSelfDirtinessTracker;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.trueFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Algorithms related to foreign key constraint transparency
 *
 * @author Gavin King
 */
public final class ForeignKeys {

	/**
	 * Delegate for handling nullifying ("null"ing-out) non-cascaded associations
	 */
	public static class Nullifier {
		private final boolean isDelete;
		private final boolean isEarlyInsert;
		private final SessionImplementor session;
		private final Object self;
		private final EntityPersister persister;

		/**
		 * Constructs a Nullifier
		 *
		 * @param self The entity
		 * @param isDelete Are we in the middle of a delete action?
		 * @param isEarlyInsert Is this an early insert (INSERT generated id strategy)?
		 * @param session The session
		 * @param persister The EntityPersister for {@code self}
		 */
		public Nullifier(
				final Object self,
				final boolean isDelete,
				final boolean isEarlyInsert,
				final SessionImplementor session,
				final EntityPersister persister) {
			this.isDelete = isDelete;
			this.isEarlyInsert = isEarlyInsert;
			this.session = session;
			this.persister = persister;
			this.self = self;
		}

		/**
		 * Nullify all references to entities that have not yet been inserted in the database, where the foreign key
		 * points toward that entity.
		 *
		 * @param values The entity attribute values
		 */
		public CompletionStage<Void> nullifyTransientReferences(final Object[] values) {
			final String[] propertyNames = persister.getPropertyNames();
			final Type[] types = persister.getPropertyTypes();
			CompletionStage<Void> loop = voidFuture();
			for ( int i = 0; i < values.length; i++ ) {
				final int index = i;
				loop = loop
						.thenCompose( v -> nullifyTransientReferences( values[index], propertyNames[index], types[index] ) )
						.thenAccept( replacement -> values[index] = replacement );
			}
			return loop;
		}

		/**
		 * Return null if the argument is an "unsaved" entity (ie. one with no existing database row), or the
		 * input argument otherwise.  This is how Hibernate avoids foreign key constraint violations.
		 *
		 * @param value An entity attribute value
		 * @param propertyName An entity attribute name
		 * @param type An entity attribute type
		 *
		 * @return {@code null} if the argument is an unsaved entity; otherwise return the argument.
		 */
		private CompletionStage<Object> nullifyTransientReferences(Object value, String propertyName, Type type) {
			CompletionStage<Object> returnedStage;
			if ( value == null ) {
				returnedStage = nullFuture();
			}
			else if ( type.isEntityType() ) {
				final EntityType entityType = (EntityType) type;
				if ( entityType.isOneToOne() ) {
					returnedStage = completedFuture( value );
				}
				else {
					// if we're dealing with a lazy property, it may need to be
					// initialized to determine if the value is nullifiable
					if ( needToInitialize( value, entityType ) ) {
						returnedStage = ( (ReactiveEntityPersister) persister )
								.reactiveInitializeLazyProperty( propertyName, self, session )
								.thenCompose( possiblyInitializedValue -> {
									if ( possiblyInitializedValue == null ) {
										// The uninitialized value was initialized to null
										return nullFuture();
									}
									else {
										// If the value is not nullifiable, make sure that the
										// possibly initialized value is returned.
										return isNullifiable( entityType.getAssociatedEntityName(), value )
												.thenApply( trans -> trans ? null : value );
									} }
								);
					}
					else {
						returnedStage = isNullifiable( entityType.getAssociatedEntityName(), value )
								.thenApply( trans -> trans ? null : value );
					}

				}
			}
			else if ( type.isAnyType() ) {
				returnedStage = isNullifiable( null, value ).thenApply( trans -> trans ? null : value );
			}
			else if ( type.isComponentType() ) {
				final CompositeType compositeType = (CompositeType) type;
				final Object[] subValues = compositeType.getPropertyValues( value, session );
				final Type[] subtypes = compositeType.getSubtypes();
				final String[] subPropertyNames = compositeType.getPropertyNames();
				CompletionStage<Boolean> loop = falseFuture();
				for ( int i = 0; i < subValues.length; i++ ) {
					final int index = i;
					loop = loop.thenCompose( substitute -> nullifyTransientReferences(
									subValues[index],
									StringHelper.qualify( propertyName, subPropertyNames[index] ),
									subtypes[index]
							)
							.thenApply( replacement -> {
								if ( replacement != subValues[index] ) {
									subValues[index] = replacement;
									return true;
								}
								else {
									return substitute;
								}
							} )
					);
				}
				returnedStage = loop.thenApply( substitute -> {
					if ( substitute ) {
						// todo : need to account for entity mode on the CompositeType interface :(
						compositeType.setPropertyValues( value, subValues );
					}
					return value;
				} );
			}
			else {
				returnedStage = completedFuture( value );
			}

			return returnedStage.thenApply( returnedValue -> {
				// value != returnedValue if either:
				// 1) returnedValue was nullified (set to null);
				// or 2) returnedValue was initialized, but not nullified.
				// When bytecode-enhancement is used for dirty-checking, the change should
				// only be tracked when returnedValue was nullified (1)).
				if ( value != returnedValue && returnedValue == null ) {
					processIfSelfDirtinessTracker( self, SelfDirtinessTracker::$$_hibernate_trackChange, propertyName );
				}
				return returnedValue;
			} );
		}

		private boolean needToInitialize(Object value, Type type) {
			return isDelete
				&& value == LazyPropertyInitializer.UNFETCHED_PROPERTY
				&& type.isEntityType()
				&& !session.getPersistenceContextInternal().isNullifiableEntityKeysEmpty();
		}

		/**
		 * Determine if the object already exists in the database,
		 * using a "best guess"
		 *
		 * @param entityName The name of the entity
		 * @param object The entity instance
		 */
		private CompletionStage<Boolean> isNullifiable(final String entityName, Object object) {
			if ( object == LazyPropertyInitializer.UNFETCHED_PROPERTY ) {
				// this is the best we can do...
				return falseFuture();
			}

			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();

			final LazyInitializer lazyInitializer = HibernateProxy.extractLazyInitializer( object );
			if ( lazyInitializer != null ) {
				// if it's an uninitialized proxy it can only be
				// transient if we did an unloaded-delete on the
				// proxy itself, in which case there is no entry
				// for it, but its key has already been registered
				// as nullifiable
				Object entity = lazyInitializer.getImplementation( session );
				if ( entity == null ) {
					// an unloaded proxy might be scheduled for deletion
					completedFuture( persistenceContext.containsDeletedUnloadedEntityKey(
							session.generateEntityKey(
									lazyInitializer.getIdentifier(),
									session.getFactory().getMappingMetamodel()
											.getEntityDescriptor( lazyInitializer.getEntityName() )
							)
					) );
				}
				else {
					//unwrap it
					object = entity;
				}
			}

			// if it was a reference to self, don't need to nullify
			// unless we are using native id generation, in which
			// case we definitely need to nullify
			if ( object == self ) {
				 return completedFuture( isEarlyInsert
						 || isDelete && session.getJdbcServices().getDialect().hasSelfReferentialForeignKeyBug() );
			}

			// See if the entity is already bound to this session, if not look at the
			// entity identifier and assume that the entity is persistent if the
			// id is not "unsaved" (that is, we rely on foreign keys to keep
			// database integrity)

			final EntityEntry entityEntry = persistenceContext.getEntry( object );
			return entityEntry == null
					? isTransient( entityName, object, null, session )
					: completedFuture( entityEntry.isNullifiable( isEarlyInsert, session ) );
		}
	}

	/**
	 * Is this instance persistent or detached?
	 * <p/>
	 * If <tt>assumed</tt> is non-null, don't hit the database to make the determination, instead assume that
	 * value; the client code must be prepared to "recover" in the case that this assumed result is incorrect.
	 *
	 * @param entityName The name of the entity
	 * @param entity The entity instance
	 * @param assumed The assumed return value, if avoiding database hit is desired
	 * @param session The session
	 *
	 * @return {@code true} if the given entity is not transient (meaning it is either detached/persistent)
	 */
	public static CompletionStage<Boolean> isNotTransient(String entityName, Object entity, Boolean assumed, SessionImplementor session) {
		if ( isHibernateProxy( entity ) ) {
			return trueFuture();
		}

		if ( session.getPersistenceContextInternal().isEntryFor( entity ) ) {
			return trueFuture();
		}

		// todo : shouldn't assumed be revered here?
		return isTransient( entityName, entity, assumed, session )
				.thenApply( trans -> !trans );
	}

	/**
	 * Is this instance, which we know is not persistent, actually transient?
	 * <p/>
	 * If <tt>assumed</tt> is non-null, don't hit the database to make the determination, instead assume that
	 * value; the client code must be prepared to "recover" in the case that this assumed result is incorrect.
	 *
	 * @param entityName The name of the entity
	 * @param entity The entity instance
	 * @param assumed The assumed return value, if avoiding database hit is desired
	 * @param session The session
	 *
	 * @return {@code true} if the given entity is transient (unsaved)
	 */
	public static CompletionStage<Boolean> isTransient(String entityName, Object entity, Boolean assumed, SessionImplementor session) {
		if ( entity == LazyPropertyInitializer.UNFETCHED_PROPERTY ) {
			// an unfetched association can only point to
			// an entity that already exists in the db
			return falseFuture();
		}

		// let the interceptor inspect the instance to decide
		Boolean isUnsaved = session.getInterceptor().isTransient( entity );
		if ( isUnsaved != null ) {
			return completedFuture( isUnsaved );
		}

		// let the persister inspect the instance to decide
		final EntityPersister persister = session.getEntityPersister( entityName, entity );
		isUnsaved = persister.isTransient( entity, session );
		if ( isUnsaved != null ) {
			return completedFuture( isUnsaved );
		}

		// we use the assumed value, if there is one, to avoid hitting
		// the database
		if ( assumed != null ) {
			return completedFuture( assumed );
		}

		// hit the database, after checking the session cache for a snapshot
		ReactivePersistenceContextAdapter persistenceContext =
				(ReactivePersistenceContextAdapter) session.getPersistenceContextInternal();
		Object id = persister.getIdentifier( entity, session );
		return persistenceContext.reactiveGetDatabaseSnapshot( id, persister ).thenApply( Objects::isNull );
	}

	/**
	 * Return the identifier of the persistent or transient object, or throw
	 * an exception if the instance is "unsaved"
	 * <p/>
	 * Used by OneToOneType and ManyToOneType to determine what id value should
	 * be used for an object that may or may not be associated with the session.
	 * This does a "best guess" using any/all info available to use (not just the
	 * EntityEntry).
	 *
	 * @param entityName The name of the entity
	 * @param object The entity instance
	 * @param session The session
	 *
	 * @return The identifier
	 *
	 * @throws TransientObjectException if the entity is transient (does not yet have an identifier)
	 */
	public static CompletionStage<Object> getEntityIdentifierIfNotUnsaved(
			final String entityName,
			final Object object,
			final SessionImplementor session) throws TransientObjectException {
		if ( object == null ) {
			return nullFuture();
		}
		else {
			Object id = session.getContextEntityIdentifier( object );
			if ( id == null ) {
				// context-entity-identifier returns null explicitly if the entity
				// is not associated with the persistence context; so make some
				// deeper checks...
				return isTransient( entityName, object, Boolean.FALSE, session )
						.thenApply( trans -> {
							if ( trans ) {
								throw new TransientObjectException(
										"object references an unsaved transient instance - save the transient instance before flushing: " +
												(entityName == null ? session.guessEntityName( object ) : entityName)
								);
							}
							return session.getEntityPersister( entityName, object )
									.getIdentifier( object, session );
						} );
			}
			else {
				return completedFuture( id );
			}
		}
	}

	public static CompletionStage<NonNullableTransientDependencies> findNonNullableTransientEntities(
			String entityName,
			Object entity,
			Object[] values,
			boolean isEarlyInsert,
			SharedSessionContractImplementor session) {

		final EntityPersister persister = session.getEntityPersister( entityName, entity );
		final Type[] types = persister.getPropertyTypes();
		final Nullifier nullifier = new Nullifier( entity, false, isEarlyInsert, (SessionImplementor) session, persister );
		final String[] propertyNames = persister.getPropertyNames();
		final boolean[] nullability = persister.getPropertyNullability();
		final NonNullableTransientDependencies nonNullableTransientEntities = new NonNullableTransientDependencies();

		return loop( 0, types.length,
				i -> collectNonNullableTransientEntities(
						nullifier,
						values[i],
						propertyNames[i],
						types[i],
						nullability[i],
						session,
						nonNullableTransientEntities
				)
		).thenApply( r -> nonNullableTransientEntities.isEmpty() ? null : nonNullableTransientEntities );
	}

	private static CompletionStage<Void> collectNonNullableTransientEntities(
			Nullifier nullifier,
			Object value,
			String propertyName,
			Type type,
			boolean isNullable,
			SharedSessionContractImplementor session,
			NonNullableTransientDependencies nonNullableTransientEntities) {

		if ( value == null ) {
			return voidFuture();
		}

		if ( type.isEntityType() ) {
			final EntityType entityType = (EntityType) type;
			if ( !isNullable && !entityType.isOneToOne() ) {
				return nullifier
						.isNullifiable( entityType.getAssociatedEntityName(), value )
						.thenAccept( nullifiable -> {
							if ( nullifiable ) {
								nonNullableTransientEntities.add( propertyName, value );
							}
						} );
			}
		}
		else if ( type.isAnyType() ) {
			if ( !isNullable ) {
				return nullifier
						.isNullifiable( null, value )
						.thenAccept( nullifiable -> {
							if ( nullifiable ) {
								nonNullableTransientEntities.add( propertyName, value );
							}
						} );
			}
		}
		else if ( type.isComponentType() ) {
			final CompositeType compositeType = (CompositeType) type;
			final boolean[] subValueNullability = compositeType.getPropertyNullability();
			if ( subValueNullability != null ) {
				final String[] subPropertyNames = compositeType.getPropertyNames();
				final Object[] subvalues = compositeType.getPropertyValues( value, session );
				final Type[] subtypes = compositeType.getSubtypes();
				return loop( 0, subtypes.length,
						i -> collectNonNullableTransientEntities(
								nullifier,
								subvalues[i],
								subPropertyNames[i],
								subtypes[i],
								subValueNullability[i],
								session,
								nonNullableTransientEntities
						)
				);
			}
		}

		return voidFuture();
	}

	/**
	 * Disallow instantiation
	 */
	private ForeignKeys() {
	}

}
