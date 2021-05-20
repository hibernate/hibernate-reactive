/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.EntityMode;
import org.hibernate.HibernateException;
import org.hibernate.TransientObjectException;
import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SelfDirtinessTracker;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.type.CompositeType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.*;

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
		public CompletionStage<Void> nullifyTransientReferences(Object[] values) {
			CompletionStage<Void> result = nullifyTransientReferences(
					null,
					values,
					persister.getPropertyTypes(),
					persister.getPropertyNames()
			);
			return result==null ? voidFuture() : result;
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
			final CompletionStage<Object> result;
			if ( value == null ) {
				return null;
			}
			else if ( type.isEntityType() ) {
				final EntityType entityType = (EntityType) type;
				if ( entityType.isOneToOne() ) {
					return null;
				}
				else {
					// if we're dealing with a lazy property, it may need to be
					// initialized to determine if the value is nullifiable
					CompletionStage<Object> fetcher;
					if ( isDelete
							&& value == LazyPropertyInitializer.UNFETCHED_PROPERTY
							&& !session.getPersistenceContextInternal().isNullifiableEntityKeysEmpty() ) {
						throw new UnsupportedOperationException("lazy property initialization not supported");
		//				fetcher = ( (LazyPropertyInitializer) persister ).initializeLazyProperty( propertyName, self, session );
					}
					else {
						fetcher = completedFuture( value );
					}
					result = fetcher.thenCompose( fetchedValue -> {
						if ( fetchedValue == null ) {
							// The uninitialized value was initialized to null
							return nullFuture();
						}
						else {
							// If the value is not nullifiable, make sure that the
							// possibly initialized value is returned.
							return isNullifiable( entityType.getAssociatedEntityName(), fetchedValue )
									.thenApply( trans -> trans ? null : fetchedValue );
						}
					} );
				}
			}
			else if ( type.isAnyType() ) {
				result = isNullifiable( null, value ).thenApply( trans -> trans  ? null : value );
			}
			else if ( type.isComponentType() ) {
				final CompositeType actype = (CompositeType) type;
				final Object[] values = actype.getPropertyValues( value, session );
				CompletionStage<Void> nullifier = nullifyTransientReferences(
						propertyName,
						values,
						actype.getSubtypes(),
						actype.getPropertyNames()
				);
				if ( nullifier == null ) {
					return null;
				}
				else {
					result = nullifier.thenAccept( v -> actype.setPropertyValues( value, values, EntityMode.POJO ) )
							.thenApply( v -> value );
				}
			}
			else {
				return null;
			}

			return result.thenApply( returnedValue -> {
				trackDirt( value, propertyName, returnedValue );
				return returnedValue;
			} );
		}

		private CompletionStage<Void> nullifyTransientReferences(String propertyName, Object[] values, Type[] types, String[] names) {
			CompletionStage<Void> nullifiers = null;
			for ( int i = 0; i < values.length; i++ ) {
				int ii = i;
				String name = propertyName==null ? names[ii] : StringHelper.qualify( propertyName, names[ii] );
				CompletionStage<Object> nullifier = nullifyTransientReferences( values[ii], name, types[ii] );
				if ( nullifier != null ) {
					nullifiers = ( nullifiers == null ? nullifier : nullifiers.thenCompose( v-> nullifier ) )
							.thenAccept( replacement -> values[ii] = replacement );
				}
			}
			return nullifiers;
		}

		private void trackDirt(Object value, String propertyName, Object returnedValue) {
			// value != returnedValue if either:
			// 1) returnedValue was nullified (set to null);
			// or 2) returnedValue was initialized, but not nullified.
			// When bytecode-enhancement is used for dirty-checking, the change should
			// only be tracked when returnedValue was nullified (1)).
			if ( value != returnedValue && returnedValue == null
					&& self instanceof SelfDirtinessTracker ) {
				( (SelfDirtinessTracker) self ).$$_hibernate_trackChange( propertyName );
			}
		}

		/**
		 * Determine if the object already exists in the database,
		 * using a "best guess"
		 *
		 * @param entityName The name of the entity
		 * @param object The entity instance
		 */
		private CompletionStage<Boolean> isNullifiable(final String entityName, Object object)
				throws HibernateException {
			if ( object == LazyPropertyInitializer.UNFETCHED_PROPERTY ) {
				// this is the best we can do...
				return falseFuture();
			}

			if ( object instanceof HibernateProxy ) {
				// if its an uninitialized proxy it can't be transient
				final LazyInitializer li = ( (HibernateProxy) object ).getHibernateLazyInitializer();
				if ( li.getImplementation( session ) == null ) {
					return falseFuture();
					// ie. we never have to null out a reference to
					// an uninitialized proxy
				}
				else {
					//unwrap it
					object = li.getImplementation( session );
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

			final EntityEntry entityEntry = session.getPersistenceContextInternal().getEntry( object );
			if ( entityEntry == null ) {
				return isTransient( entityName, object, null, session );
			}
			else {
				return completedFuture( entityEntry.isNullifiable( isEarlyInsert, session ) );
			}
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
	@SuppressWarnings("SimplifiableIfStatement")
	public static CompletionStage<Boolean> isNotTransient(String entityName, Object entity, Boolean assumed, SessionImplementor session) {
		if ( entity instanceof HibernateProxy ) {
			return trueFuture();
		}

		if ( session.getPersistenceContextInternal().isEntryFor( entity ) ) {
			return trueFuture();
		}

		// todo : shouldnt assumed be revered here?

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
	public static CompletionStage<Boolean> isTransient(String entityName, Object entity, Boolean assumed,
													   SessionImplementor session) {
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
		Serializable id = persister.getIdentifier(entity, session);
		return persistenceContext.reactiveGetDatabaseSnapshot( id, persister).thenApply(Objects::isNull);
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
	public static CompletionStage<Serializable> getEntityIdentifierIfNotUnsaved(
			final String entityName,
			final Object object,
			final SessionImplementor session) throws TransientObjectException {
		if ( object == null ) {
			return nullFuture();
		}
		else {
			Serializable id = session.getContextEntityIdentifier( object );
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


	/**
	 * Disallow instantiation
	 */
	private ForeignKeys() {
	}

}
