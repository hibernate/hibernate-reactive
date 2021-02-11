/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.CacheMode;
import org.hibernate.Hibernate;
import org.hibernate.HibernateException;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.spi.CascadeStyle;
import org.hibernate.engine.spi.CollectionEntry;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.type.AssociationType;
import org.hibernate.type.CollectionType;
import org.hibernate.type.CompositeType;
import org.hibernate.type.EntityType;
import org.hibernate.type.ForeignKeyDirection;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Delegate responsible for, in conjunction with the various
 * {@link CascadingAction actions}, implementing cascade processing.
 * This is a reactive counterpart to Hibernate's
 * {@link org.hibernate.engine.internal.Cascade}.
 *
 * @author Gavin King
 * @see CascadingAction
 */
public final class Cascade<C> {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( Cascade.class );

	private final CascadingAction<C> action;
	private final EntityPersister persister;
	private final Object parent;
	private final EventSource eventSource;
	private final C context;
	private CascadePoint cascadePoint;

	private CompletionStage<Void> stage = voidFuture();

	/**
	 * 	@param persister The parent's entity persister
	 * 	@param parent The parent reference.
	 */
	public Cascade(final CascadingAction<C> action,
				   final CascadePoint cascadePoint,
				   final EntityPersister persister,
				   final Object parent,
				   final C context,
				   final EventSource eventSource) {
		this.action = action;
		this.parent = parent;
		this.persister = persister;
		this.cascadePoint = cascadePoint;
		this.eventSource = eventSource;
		this.context = context;
	}

	public static CompletionStage<?> fetchLazyAssociationsBeforeCascade(
			CascadingAction<?> action,
			EntityPersister persister,
			Object entity,
			EventSource session) {

		CompletionStage<?> beforeDelete = voidFuture();
		if ( persister.hasCascades() ) {
			CascadeStyle[] cascadeStyles = persister.getPropertyCascadeStyles();
			Object[] state = persister.getPropertyValues( entity );
			for (int i = 0; i < cascadeStyles.length; i++) {
				if ( cascadeStyles[i].doCascade( action.delegate() ) ) {
					Object fetchable = state[i];
					if ( !Hibernate.isInitialized( fetchable ) ) {
						beforeDelete = beforeDelete.thenCompose( v -> session.unwrap(ReactiveSession.class).reactiveFetch( fetchable, true ) );
					}
				}
			}
		}
		return beforeDelete;
	}

	/**
	 * Cascade an action from the parent entity instance to all its children.
	 *
	 * which is specific to each CascadingAction type
	 */
	public CompletionStage<Void> cascade() throws HibernateException {
		return voidFuture().thenCompose(v -> {
			CacheMode cacheMode = eventSource.getCacheMode();
			if (action==CascadingActions.DELETE) {
				eventSource.setCacheMode( CacheMode.GET );
			}
			eventSource.getPersistenceContextInternal().incrementCascadeLevel();
			return cascadeInternal().whenComplete( (vv, e) -> {
				eventSource.getPersistenceContextInternal().decrementCascadeLevel();
				eventSource.setCacheMode( cacheMode );
			} );
		} );
	}

	private CompletionStage<Void> cascadeInternal() throws HibernateException {

		if ( persister.hasCascades() || action.requiresNoCascadeChecking() ) { // performance opt
			final boolean traceEnabled = LOG.isTraceEnabled();
			if ( traceEnabled ) {
				LOG.tracev( "Processing cascade {0} for: {1}", action, persister.getEntityName() );
			}
			final PersistenceContext persistenceContext = eventSource.getPersistenceContextInternal();

			final Type[] types = persister.getPropertyTypes();
			final String[] propertyNames = persister.getPropertyNames();
			final CascadeStyle[] cascadeStyles = persister.getPropertyCascadeStyles();
			final boolean hasUninitializedLazyProperties = persister.hasUninitializedLazyProperties( parent );
			final int componentPathStackDepth = 0;
			for ( int i = 0; i < types.length; i++) {
				final CascadeStyle style = cascadeStyles[ i ];
				final String propertyName = propertyNames[ i ];
				final boolean isUninitializedProperty =
						hasUninitializedLazyProperties &&
						!persister.getBytecodeEnhancementMetadata().isAttributeLoaded( parent, propertyName );

				if ( style.doCascade( action.delegate() ) ) {
					final Object child;
					if ( isUninitializedProperty  ) {
						// parent is a bytecode enhanced entity.
						// Cascade to an uninitialized, lazy value only if
						// parent is managed in the PersistenceContext.
						// If parent is a detached entity being merged,
						// then parent will not be in the PersistenceContext
						// (so lazy attributes must not be initialized).
						if ( persistenceContext.getEntry( parent ) == null ) {
							// parent was not in the PersistenceContext
							continue;
						}
						if ( types[ i ].isCollectionType() ) {
							// CollectionType#getCollection gets the PersistentCollection
							// that corresponds to the uninitialized collection from the
							// PersistenceContext. If not present, an uninitialized
							// PersistentCollection will be added to the PersistenceContext.
							// The action may initialize it later, if necessary.
							// This needs to be done even when action.performOnLazyProperty() returns false.
							final CollectionType collectionType = (CollectionType) types[i];
							child = collectionType.getCollection(
									collectionType.getKeyOfOwner( parent, eventSource ),
									eventSource,
									parent,
									null
							);
						}
						else if ( types[ i ].isComponentType() ) {
							// Hibernate does not support lazy embeddables, so this shouldn't happen.
							throw new UnsupportedOperationException(
									"Lazy components are not supported."
							);
						}
						else if ( action.performOnLazyProperty() && types[ i ].isEntityType() ) {
							// Only need to initialize a lazy entity attribute when action.performOnLazyProperty()
							// returns true.
							LazyAttributeLoadingInterceptor interceptor = persister.getBytecodeEnhancementMetadata()
									.extractInterceptor( parent );
							child = interceptor.fetchAttribute( parent, propertyName );

						}
						else {
							// Nothing to do, so just skip cascading to this lazy attribute.
							continue;
						}
					}
					else {
						child = persister.getPropertyValue( parent, i );
					}
					cascadeProperty(
							componentPathStackDepth,
							child,
							types[ i ],
							style,
							propertyName,
							false
					);
				}
				else {
					if ( action.requiresNoCascadeChecking() ) {
						noCascade( eventSource, parent, persister, types, i );
					}
					// If the property is uninitialized, then there cannot be any orphans.
					if ( action.deleteOrphans() && !isUninitializedProperty ) {
						cascadeLogicalOneToOneOrphanRemoval(
								componentPathStackDepth,
								persister.getPropertyValue( parent, i ),
								types[ i ],
								style,
								propertyName,
								false
						);
					}
				}
			}

			if ( traceEnabled ) {
				LOG.tracev( "Done processing cascade {0} for: {1}", action, persister.getEntityName() );
			}
		}

		return stage;
	}

	private void noCascade(
			final EventSource eventSource,
			final Object parent,
			final EntityPersister persister,
			final Type[] types,
			final int i) {
		stage = stage.thenCompose( v -> action.noCascade( eventSource, parent, persister, types[i], i ) );
	}

	/**
	 * Cascade an action to the child or children
	 */
	private void cascadeProperty(
			final int componentPathStackDepth,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final String propertyName,
			final boolean isCascadeDeleteEnabled) throws HibernateException {

		if ( child != null ) {
			if ( type.isAssociationType() ) {
				final AssociationType associationType = (AssociationType) type;
				if ( cascadeAssociationNow( cascadePoint, associationType ) ) {
					cascadeAssociation(
							componentPathStackDepth,
							child,
							type,
							style,
							isCascadeDeleteEnabled
						);
				}
			}
			else if ( type.isComponentType() ) {
				cascadeComponent(
						componentPathStackDepth,
						child,
						(CompositeType) type
				);
			}
		}

		cascadeLogicalOneToOneOrphanRemoval(
				componentPathStackDepth,
				child,
				type,
				style,
				propertyName,
				isCascadeDeleteEnabled );
	}

	private void cascadeLogicalOneToOneOrphanRemoval(
			final int componentPathStackDepth,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final String propertyName,
			final boolean isCascadeDeleteEnabled) throws HibernateException {

		// potentially we need to handle orphan deletes for one-to-ones here...
		if ( isLogicalOneToOne( type ) ) {
			// We have a physical or logical one-to-one.  See if the attribute cascade settings and action-type require
			// orphan checking
			if ( style.hasOrphanDelete() && action.deleteOrphans() ) {
				// value is orphaned if loaded state for this property shows not null
				// because it is currently null.
				final PersistenceContext persistenceContext = eventSource.getPersistenceContextInternal();
				final EntityEntry entry = persistenceContext.getEntry( parent );
				if ( entry != null && entry.getStatus() != Status.SAVING ) {
					Object loadedValue;
					if ( componentPathStackDepth == 0 ) {
						// association defined on entity
						loadedValue = entry.getLoadedValue( propertyName );
					}
					else {
						// association defined on component
						// 		todo : this is currently unsupported because of the fact that
						//		we do not know the loaded state of this value properly
						//		and doing so would be very difficult given how components and
						//		entities are loaded (and how 'loaded state' is put into the
						//		EntityEntry).  Solutions here are to either:
						//			1) properly account for components as a 2-phase load construct
						//			2) just assume the association was just now orphaned and
						// 				issue the orphan delete.  This would require a special
						//				set of SQL statements though since we do not know the
						//				orphaned value, something a delete with a subquery to
						// 				match the owner.
//							final EntityType entityType = (EntityType) type;
//							final String getPropertyPath = composePropertyPath( entityType.getPropertyName() );
						loadedValue = null;
					}

					// orphaned if the association was nulled (child == null) or receives a new value while the
					// entity is managed (without first nulling and manually flushing).
					if ( child == null || ( loadedValue != null && child != loadedValue ) ) {
						EntityEntry valueEntry = persistenceContext.getEntry( loadedValue );

						if ( valueEntry == null && loadedValue instanceof HibernateProxy ) {
							// un-proxy and re-associate for cascade operation
							// useful for @OneToOne defined as FetchType.LAZY
							loadedValue = persistenceContext.unproxyAndReassociate( loadedValue );
							valueEntry = persistenceContext.getEntry( loadedValue );

							// HHH-11965
							// Should the unwrapped proxy value be equal via reference to the entity's property value
							// provided by the 'child' variable, we should not trigger the orphan removal of the
							// associated one-to-one.
							if ( child == loadedValue ) {
								// do nothing
								return;
							}
						}

						if ( valueEntry != null ) {
							final String entityName = valueEntry.getPersister().getEntityName();
							if ( LOG.isTraceEnabled() ) {
								final Serializable id = valueEntry.getPersister().getIdentifier( loadedValue, eventSource );
								final String description = MessageHelper.infoString( entityName, id );
								LOG.tracev( "Deleting orphaned entity instance: {0}", description );
							}

							if ( type.isAssociationType() && ( (AssociationType) type ).getForeignKeyDirection().equals(
									ForeignKeyDirection.TO_PARENT
							) ) {
								// If FK direction is to-parent, we must remove the orphan *before* the queued update(s)
								// occur.  Otherwise, replacing the association on a managed entity, without manually
								// nulling and flushing, causes FK constraint violations.
								eventSource.removeOrphanBeforeUpdates( entityName, loadedValue );
							}
							else {
								// Else, we must delete after the updates.
								eventSource.delete( entityName, loadedValue, isCascadeDeleteEnabled, new HashSet<>() );
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Check if the association is a one to one in the logical model (either a shared-pk
	 * or unique fk).
	 *
	 * @param type The type representing the attribute metadata
	 *
	 * @return True if the attribute represents a logical one to one association
	 */
	private  boolean isLogicalOneToOne(Type type) {
		return type.isEntityType() && ( (EntityType) type ).isLogicalOneToOne();
	}

	private  boolean cascadeAssociationNow(final CascadePoint cascadePoint, AssociationType associationType) {
		return associationType.getForeignKeyDirection().cascadeNow( cascadePoint );
	}

	private void cascadeComponent(
			final int componentPathStackDepth,
			final Object child,
			final CompositeType componentType) {

		Object[] children = null;
		final Type[] types = componentType.getSubtypes();
		final String[] propertyNames = componentType.getPropertyNames();
		for ( int i = 0; i < types.length; i++ ) {
			final CascadeStyle componentPropertyStyle = componentType.getCascadeStyle( i );
			final String subPropertyName = propertyNames[i];
			if ( componentPropertyStyle.doCascade( action.delegate() ) ) {
				if (children == null) {
					// Get children on demand.
					children = componentType.getPropertyValues( child, eventSource );
				}
				cascadeProperty(
						componentPathStackDepth + 1,
						children[i],
						types[i],
						componentPropertyStyle,
						subPropertyName,
						false
					);
			}
		}
	}

	private void cascadeAssociation(
			final int componentPathStackDepth,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final boolean isCascadeDeleteEnabled) {
		if ( type.isEntityType() || type.isAnyType() ) {
			cascadeToOne( child, type, style, isCascadeDeleteEnabled );
		}
		else if ( type.isCollectionType() ) {
			cascadeCollection(
					componentPathStackDepth,
					child,
					style,
					(CollectionType) type
			);
		}
	}

	/**
	 * Cascade an action to a collection
	 */
	private void cascadeCollection(
			final int componentPathStackDepth,
			final Object child,
			final CascadeStyle style,
			final CollectionType type) {
		final CollectionPersister persister =
				eventSource.getFactory().getMetamodel().collectionPersister( type.getRole() );
		final Type elemType = persister.getElementType();

		CascadePoint elementsCascadePoint = cascadePoint;
		if ( cascadePoint == CascadePoint.AFTER_INSERT_BEFORE_DELETE ) {
			cascadePoint = CascadePoint.AFTER_INSERT_BEFORE_DELETE_VIA_COLLECTION;
		}

		//cascade to current collection elements
		if ( elemType.isEntityType() || elemType.isAnyType() || elemType.isComponentType() ) {
			cascadeCollectionElements(
				componentPathStackDepth,
				child,
				type,
				style,
				elemType,
				persister.isCascadeDeleteEnabled()
			);
		}

		cascadePoint = elementsCascadePoint;
	}

	/**
	 * Cascade an action to a to-one association or any type
	 */
	private void cascadeToOne(
			final Object child,
			final Type type,
			final CascadeStyle style,
			final boolean isCascadeDeleteEnabled) {
		final String entityName = type.isEntityType()
				? ( (EntityType) type ).getAssociatedEntityName()
				: null;
		if ( style.reallyDoCascade( action.delegate() ) ) {
			//not really necessary, but good for consistency...
			final PersistenceContext persistenceContext = eventSource.getPersistenceContextInternal();
			persistenceContext.addChildParent( child, parent );
			stage = stage.thenCompose( v -> action.cascade( eventSource, child, entityName, context, isCascadeDeleteEnabled ) )
					.whenComplete( (vv, e) -> persistenceContext.removeChildParent( child ) )
					.thenAccept( vv -> {} );
		}
	}

	/**
	 * Cascade to the collection elements
	 */
	private void cascadeCollectionElements(
			final int componentPathStackDepth,
			final Object child,
			final CollectionType collectionType,
			final CascadeStyle style,
			final Type elemType,
			final boolean isCascadeDeleteEnabled) throws HibernateException {
		final boolean reallyDoCascade = style.reallyDoCascade( action.delegate() )
				&& child != CollectionType.UNFETCHED_COLLECTION;

		if ( reallyDoCascade ) {
			final boolean traceEnabled = LOG.isTraceEnabled();
			if ( traceEnabled ) {
				LOG.tracev( "Cascade {0} for collection: {1}", action, collectionType.getRole() );
			}

			final Iterator<?> itr = action.getCascadableChildrenIterator( eventSource, collectionType, child );
			while ( itr.hasNext() ) {
				cascadeProperty(
						componentPathStackDepth,
						itr.next(),
						elemType,
						style,
						null,
						isCascadeDeleteEnabled
				);
			}

			if ( traceEnabled ) {
				LOG.tracev( "Done cascade {0} for collection: {1}", action, collectionType.getRole() );
			}
		}

		final boolean deleteOrphans = style.hasOrphanDelete()
				&& action.deleteOrphans()
				&& elemType.isEntityType()
				// a newly instantiated collection can't have orphans
				&& child instanceof PersistentCollection;

		if ( deleteOrphans ) {
			final boolean traceEnabled = LOG.isTraceEnabled();
			if ( traceEnabled ) {
				LOG.tracev( "Deleting orphans for collection: {0}", collectionType.getRole() );
			}
			// we can do the cast since orphan-delete does not apply to:
			// 1. newly instantiated collections
			// 2. arrays (we can't track orphans for detached arrays)
			final String entityName = collectionType.getAssociatedEntityName( eventSource.getFactory() );
			deleteOrphans( entityName, (PersistentCollection) child );

			if ( traceEnabled ) {
				LOG.tracev( "Done deleting orphans for collection: {0}", collectionType.getRole() );
			}
		}
	}

	/**
	 * Delete any entities that were removed from the collection
	 */
	private void deleteOrphans(String entityName, PersistentCollection pc) throws HibernateException {
		//TODO: suck this logic into the collection!
		final Collection<?> orphans;
		if ( pc.wasInitialized() ) {
			final CollectionEntry ce = eventSource.getPersistenceContextInternal().getCollectionEntry( pc );
			orphans = ce==null
					? Collections.EMPTY_LIST
					: ce.getOrphans( entityName, pc );
		}
		else {
			orphans = pc.getQueuedOrphans( entityName );
		}

		ReactiveSession session = (ReactiveSession) eventSource;
		stage = stage.thenCompose( v -> CompletionStages.loop( orphans, orphan -> {
			if ( orphan != null ) {
				LOG.tracev( "Deleting orphaned entity instance: {0}", entityName );
				return session.reactiveRemove( orphan, false, new IdentitySet() );
			}
			return voidFuture();
		} ) );
	}
}
