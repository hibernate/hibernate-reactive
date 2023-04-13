/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

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
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.EventSource;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.type.AssociationType;
import org.hibernate.type.CollectionType;
import org.hibernate.type.ComponentType;
import org.hibernate.type.CompositeType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.type.ForeignKeyDirection.TO_PARENT;

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

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

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
			Object[] state = persister.getValues( entity );
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
	 */
	public CompletionStage<Void> cascade() throws HibernateException {
		return voidFuture().thenCompose(v -> {
			CacheMode cacheMode = eventSource.getCacheMode();
			if ( action==CascadingActions.DELETE ) {
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
			for ( int i = 0; i < types.length; i++) {
				final CascadeStyle style = cascadeStyles[ i ];
				final String propertyName = propertyNames[ i ];
				final boolean isUninitializedProperty =
						hasUninitializedLazyProperties &&
						!persister.getBytecodeEnhancementMetadata().isAttributeLoaded( parent, propertyName );

				final Type type = types[i];
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
						if ( type.isCollectionType() ) {
							// CollectionType#getCollection gets the PersistentCollection
							// that corresponds to the uninitialized collection from the
							// PersistenceContext. If not present, an uninitialized
							// PersistentCollection will be added to the PersistenceContext.
							// The action may initialize it later, if necessary.
							// This needs to be done even when action.performOnLazyProperty() returns false.
							final CollectionType collectionType = (CollectionType) type;
							child = collectionType.getCollection(
									collectionType.getKeyOfOwner( parent, eventSource ),
									eventSource,
									parent,
									null
							);
						}
						else if ( type.isComponentType() ) {
							// Hibernate does not support lazy embeddables, so this shouldn't happen.
							throw new UnsupportedOperationException(
									"Lazy components are not supported."
							);
						}
						else if ( action.performOnLazyProperty() && type.isEntityType() ) {
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
						child = persister.getValue( parent, i );
					}
					cascadeProperty(
							null,
							child,
							type,
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
								null,
								persister.getValue( parent, i ),
								type,
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
			List<String> componentPath,
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
							componentPath,
							child,
							type,
							style,
							isCascadeDeleteEnabled
						);
				}
			}
			else if ( type.isComponentType() ) {
				if ( componentPath == null && propertyName != null ) {
					componentPath = new ArrayList<>();
				}
				if ( componentPath != null ) {
					componentPath.add( propertyName );
				}
				cascadeComponent(
						componentPath,
						child,
						(CompositeType) type
				);
			}
		}

		cascadeLogicalOneToOneOrphanRemoval(
				componentPath,
				child,
				type,
				style,
				propertyName,
				isCascadeDeleteEnabled );
	}

	private void cascadeLogicalOneToOneOrphanRemoval(
			final List<String> componentPath,
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
					if ( componentPath == null ) {
						// association defined on entity
						loadedValue = entry.getLoadedValue( propertyName );
					}
					else {
						// association defined on component
						// Since the loadedState in the EntityEntry is a flat domain type array
						// We first have to extract the component object and then ask the component type
						// recursively to give us the value of the sub-property of that object
						final Type propertyType = entry.getPersister().getPropertyType( componentPath.get(0) );
						if ( propertyType instanceof ComponentType) {
							loadedValue = entry.getLoadedValue( componentPath.get( 0 ) );
							ComponentType componentType = (ComponentType) propertyType;
							if ( componentPath.size() != 1 ) {
								for ( int i = 1; i < componentPath.size(); i++ ) {
									final int subPropertyIndex = componentType.getPropertyIndex( componentPath.get( i ) );
									loadedValue = componentType.getPropertyValue( loadedValue, subPropertyIndex );
									componentType = (ComponentType) componentType.getSubtypes()[subPropertyIndex];
								}
							}

							loadedValue = componentType.getPropertyValue( loadedValue, componentType.getPropertyIndex( propertyName ) );
						}
						else {
							// Association is probably defined in an element collection, so we can't do orphan removals
							loadedValue = null;
						}
					}

					// orphaned if the association was nulled (child == null) or receives a new value while the
					// entity is managed (without first nulling and manually flushing).
					if ( child == null || loadedValue != null && child != loadedValue ) {
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
							final EntityPersister persister = valueEntry.getPersister();
							final String entityName = persister.getEntityName();
							if ( LOG.isTraceEnabled() ) {
								LOG.tracev(
										"Deleting orphaned entity instance: {0}",
										infoString( entityName, persister.getIdentifier( loadedValue, eventSource ) )
								);
							}

							final Object loaded = loadedValue;
							if ( type.isAssociationType()
									&& ( (AssociationType) type ).getForeignKeyDirection().equals(TO_PARENT) ) {
								// If FK direction is to-parent, we must remove the orphan *before* the queued update(s)
								// occur.  Otherwise, replacing the association on a managed entity, without manually
								// nulling and flushing, causes FK constraint violations.
								stage = stage.thenCompose( v -> ( (ReactiveSession) eventSource )
										.reactiveRemoveOrphanBeforeUpdates( entityName, loaded ) );
							}
							else {
								// Else, we must delete after the updates.
								stage = stage.thenCompose( v -> ( (ReactiveSession) eventSource )
										.reactiveRemove( entityName, loaded, isCascadeDeleteEnabled, DeleteContext.create() ) );
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
			List<String> componentPath,
			final Object child,
			final CompositeType componentType) {

		Object[] children = null;
		final Type[] types = componentType.getSubtypes();
		final String[] propertyNames = componentType.getPropertyNames();
		for ( int i = 0; i < types.length; i++ ) {
			final CascadeStyle componentPropertyStyle = componentType.getCascadeStyle( i );
			final String subPropertyName = propertyNames[i];
			if ( componentPropertyStyle.doCascade( action.delegate() )
					|| componentPropertyStyle.hasOrphanDelete() && action.deleteOrphans() ) {
				if ( children == null ) {
					// Get children on demand.
					children = componentType.getPropertyValues( child, eventSource );
				}
				cascadeProperty(
						componentPath,
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
			List<String> componentPath,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final boolean isCascadeDeleteEnabled) {
		if ( type.isEntityType() || type.isAnyType() ) {
			cascadeToOne( child, type, style, isCascadeDeleteEnabled );
		}
		else if ( type.isCollectionType() ) {
			cascadeCollection(
					componentPath,
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
			List<String> componentPath,
			final Object child,
			final CascadeStyle style,
			final CollectionType type) {
		final CollectionPersister persister =
				eventSource.getFactory().getMappingMetamodel()
						.getCollectionDescriptor( type.getRole() );
		final Type elemType = persister.getElementType();

		CascadePoint elementsCascadePoint = cascadePoint;
		if ( cascadePoint == CascadePoint.AFTER_INSERT_BEFORE_DELETE ) {
			cascadePoint = CascadePoint.AFTER_INSERT_BEFORE_DELETE_VIA_COLLECTION;
		}

		//cascade to current collection elements
		if ( elemType.isEntityType() || elemType.isAnyType() || elemType.isComponentType() ) {
			cascadeCollectionElements(
				componentPath,
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
					.whenComplete( (vv, e) -> persistenceContext.removeChildParent( child ) );
		}
	}

	/**
	 * Cascade to the collection elements
	 */
	private void cascadeCollectionElements(
			List<String> componentPath,
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
						componentPath,
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
				&& child instanceof PersistentCollection
				// a newly instantiated collection can't have orphans
				&& ! ( (PersistentCollection<?>) child ).isNewlyInstantiated();

		if ( deleteOrphans ) {
			final boolean traceEnabled = LOG.isTraceEnabled();
			if ( traceEnabled ) {
				LOG.tracev( "Deleting orphans for collection: {0}", collectionType.getRole() );
			}
			// we can do the cast since orphan-delete does not apply to:
			// 1. newly instantiated collections
			// 2. arrays (we can't track orphans for detached arrays)
			final String entityName = collectionType.getAssociatedEntityName( eventSource.getFactory() );
			deleteOrphans( entityName, (PersistentCollection<?>) child );

			if ( traceEnabled ) {
				LOG.tracev( "Done deleting orphans for collection: {0}", collectionType.getRole() );
			}
		}
	}

	/**
	 * Delete any entities that were removed from the collection
	 */
	private void deleteOrphans(String entityName, PersistentCollection<?> pc) throws HibernateException {
		//TODO: suck this logic into the collection!
		final Collection<?> orphans;
		if ( pc.wasInitialized() ) {
			final CollectionEntry ce = eventSource.getPersistenceContextInternal().getCollectionEntry( pc );
			if ( ce == null ) {
				return;
			}
			orphans = ce.getOrphans( entityName, pc );
		}
		else {
			orphans = pc.getQueuedOrphans( entityName );
		}

		ReactiveSession session = (ReactiveSession) eventSource;
		stage = stage.thenCompose( v -> loop(
				orphans,
				Objects::nonNull,
				orphan -> {
					LOG.tracev( "Deleting orphaned entity instance: {0}", entityName );
					return session.reactiveRemove( entityName, orphan, false, DeleteContext.create() );
				}
		) );
	}
}
