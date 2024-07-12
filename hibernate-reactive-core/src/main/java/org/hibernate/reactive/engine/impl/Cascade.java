/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

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
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.EventSource;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.type.AssociationType;
import org.hibernate.type.CollectionType;
import org.hibernate.type.ComponentType;
import org.hibernate.type.CompositeType;
import org.hibernate.type.EntityType;
import org.hibernate.type.ManyToOneType;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.Type;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.EMPTY_LIST;
import static org.hibernate.engine.internal.ManagedTypeHelper.isHibernateProxy;
import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
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
public final class Cascade {

	private static final Log LOG = make( Log.class, lookup() );

	private Cascade() {
	}

	public static CompletionStage<?> fetchLazyAssociationsBeforeCascade(
			CascadingAction<?> action,
			EntityPersister persister,
			Object entity,
			EventSource session) {

		CompletionStage<?> beforeDelete = voidFuture();
		if ( persister.hasCascades() ) {
			final CascadeStyle[] cascadeStyles = persister.getPropertyCascadeStyles();
			final Object[] state = persister.getValues( entity );
			for (int i = 0; i < cascadeStyles.length; i++) {
				if ( cascadeStyles[i].doCascade( action.delegate() ) ) {
					Object fetchable = state[i];
					if ( !Hibernate.isInitialized( fetchable ) ) {
						beforeDelete = beforeDelete.thenCompose( v -> session
								.unwrap( ReactiveSession.class )
								.reactiveFetch( fetchable, true )
						);
					}
				}
			}
		}
		return beforeDelete;
	}

	public static <T> CompletionStage<Void> cascade(
			final CascadingAction<T> action,
			final CascadePoint cascadePoint,
			final EventSource eventSource,
			final EntityPersister persister,
			final Object parent,
			final T anything) throws HibernateException {
		CacheMode cacheMode = eventSource.getCacheMode();
		// In Hibernate actually set the cache before calling cascade for the remove, but this solution reduces some code
		if ( action == CascadingActions.REMOVE ) {
			eventSource.setCacheMode( CacheMode.GET );
		}
		// Hibernate ORM actually increment/decrement the level before calling cascade, but keeping it here avoid extra
		// code every time we need to cascade
		eventSource.getPersistenceContextInternal().incrementCascadeLevel();
		return voidFuture()
				.thenCompose( v -> cascadeInternal( action, cascadePoint, eventSource, persister, parent, anything ) )
				.whenComplete( (unused, throwable) -> {
					eventSource.getPersistenceContextInternal().decrementCascadeLevel();
					eventSource.setCacheMode( cacheMode );
				} );
	}

	private static <T> CompletionStage<Void> cascadeInternal(
			CascadingAction<T> action,
			CascadePoint cascadePoint,
			EventSource eventSource,
			EntityPersister persister,
			Object parent,
			T anything) {
		if ( persister.hasCascades() || action == CascadingActions.CHECK_ON_FLUSH ) { // performance opt
			final boolean traceEnabled = LOG.isTraceEnabled();
			if ( traceEnabled ) {
				LOG.tracev( "Processing cascade {0} for: {1}", action, persister.getEntityName() );
			}
			return doCascade( action, cascadePoint, eventSource, persister, parent, anything )
					.thenRun( () -> {
						if ( traceEnabled ) {
							LOG.tracev( "Done processing cascade {0} for: {1}", action, persister.getEntityName() );
						}
					} );
		}
		return voidFuture();
	}

	/**
	 * Cascade an action from the parent entity instance to all its children.
	 */
	private static <T> CompletionStage<Void> doCascade(
			final CascadingAction action,
			final CascadePoint cascadePoint,
			final EventSource eventSource,
			final EntityPersister persister,
			final Object parent,
			final T anything) throws HibernateException {
		final PersistenceContext persistenceContext = eventSource.getPersistenceContextInternal();
		final EntityEntry entry = persistenceContext.getEntry( parent );

		if ( entry != null
				&& entry.getLoadedState() == null
				&& entry.getStatus() == Status.MANAGED
				&& persister.getBytecodeEnhancementMetadata().isEnhancedForLazyLoading() ) {
			return voidFuture();
		}

		final Type[] types = persister.getPropertyTypes();
		final String[] propertyNames = persister.getPropertyNames();
		final CascadeStyle[] cascadeStyles = persister.getPropertyCascadeStyles();
		final boolean hasUninitializedLazyProperties = persister.hasUninitializedLazyProperties( parent );

		CompletionStage<Void> stage = voidFuture();
		for ( int i = 0; i < types.length; i++ ) {
			final CascadeStyle style = cascadeStyles[i];
			final String propertyName = propertyNames[i];
			final boolean isUninitializedProperty = hasUninitializedLazyProperties
					&& !persister.getBytecodeEnhancementMetadata().isAttributeLoaded( parent, propertyName );

			final Type type = types[i];
			if ( style.doCascade( action.delegate() ) ) {
				if ( isUninitializedProperty ) {
					// parent is a bytecode enhanced entity.
					// Cascade to an uninitialized, lazy value only if
					// parent is managed in the PersistenceContext.
					// If parent is a detached entity being merged,
					// then parent will not be in the PersistenceContext
					// (so lazy attributes must not be initialized).
					if ( entry == null ) {
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
						Object child = collectionType.getCollection(
								collectionType.getKeyOfOwner( parent, eventSource ),
								eventSource,
								parent,
								null
						);
						stage = stage.thenCompose( v -> cascadeProperty(
								action,
								cascadePoint,
								eventSource,
								null,
								parent,
								child,
								type,
								style,
								propertyName,
								anything,
								false
						) );
					}
					else if ( type.isComponentType() ) {
						// Hibernate does not support lazy embeddables, so this shouldn't happen.
						throw new UnsupportedOperationException( "Lazy components are not supported." );
					}
					else if ( action.performOnLazyProperty() && type.isEntityType() ) {
						// Only need to initialize a lazy entity attribute when action.performOnLazyProperty()
						// returns true.
						LazyAttributeLoadingInterceptor interceptor = persister.getBytecodeEnhancementMetadata()
								.extractInterceptor( parent );
						stage = stage
								.thenCompose( v -> (CompletionStage<Object>) interceptor
										.fetchAttribute( parent, propertyName )
								)
								.thenCompose( actualChild -> cascadeProperty(
										action,
										cascadePoint,
										eventSource,
										null,
										parent,
										actualChild,
										type,
										style,
										propertyName,
										anything,
										false
								) );
					}
					else {
						// Nothing to do, so just skip cascading to this lazy attribute.
						continue;
					}
				}
				else {
					Object child = persister.getValue( parent, i );
					stage = stage.thenCompose( v -> cascadeProperty(
							action,
							cascadePoint,
							eventSource,
							null,
							parent,
							child,
							type,
							style,
							propertyName,
							anything,
							false
					) );
				}
			}
			else {
				// If the property is uninitialized, then there cannot be any orphans.
				if ( action.deleteOrphans() && !isUninitializedProperty ) {
					final int index = i;
					stage = stage.thenCompose( v -> cascadeLogicalOneToOneOrphanRemoval(
							action,
							eventSource,
							null,
							parent,
							persister.getValue( parent, index ),
							type,
							style,
							propertyName,
							false
					) );
				}
			}
		}
		return stage;
	}

	/**
	 * Cascade an action to the child or children
	 */
	private static <T> CompletionStage<Void> cascadeProperty(
			final CascadingAction<T> action,
			final CascadePoint cascadePoint,
			final EventSource eventSource,
			List<String> componentPath,
			final Object parent,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final String propertyName,
			final T anything,
			final boolean isCascadeDeleteEnabled) throws HibernateException {

		if ( child != null ) {
			if ( type.isAssociationType() ) {
				final AssociationType associationType = (AssociationType) type;
				final boolean unownedTransient = eventSource.getSessionFactory()
						.getSessionFactoryOptions()
						.isUnownedAssociationTransientCheck();
				if ( cascadeAssociationNow( action, cascadePoint, associationType, eventSource.getFactory(), unownedTransient ) ) {
					final List<String> path = componentPath;
					return cascadeAssociation( action, cascadePoint, eventSource, path, parent, child, type, style, anything, isCascadeDeleteEnabled )
							.thenCompose( v -> cascadeLogicalOneToOne( action, eventSource, path, parent, child, type, style, propertyName, isCascadeDeleteEnabled ) );
				}
			}
			else if ( type.isComponentType() ) {
				if ( componentPath == null && propertyName != null ) {
					componentPath = new ArrayList<>();
				}
				if ( componentPath != null ) {
					componentPath.add( propertyName );
				}
				final List<String> path = componentPath;
				return cascadeComponent( action, cascadePoint, eventSource, path, parent, child, (CompositeType) type, anything )
						.thenRun( () -> {
							if ( path != null ) {
								path.remove( path.size() - 1 );
							}
						} )
						.thenCompose( v -> cascadeLogicalOneToOne( action, eventSource, path, parent, child, type, style, propertyName, isCascadeDeleteEnabled ) );
			}
		}
		return cascadeLogicalOneToOne( action, eventSource, componentPath, parent, child, type, style, propertyName, isCascadeDeleteEnabled );
	}

	private static <T> CompletionStage<Void> cascadeLogicalOneToOne(
			CascadingAction<T> action,
			EventSource eventSource,
			List<String> componentPath,
			Object parent,
			Object child,
			Type type,
			CascadeStyle style,
			String propertyName,
			boolean isCascadeDeleteEnabled) {
		return isLogicalOneToOne( type )
				? cascadeLogicalOneToOneOrphanRemoval( action, eventSource, componentPath, parent, child, type, style, propertyName, isCascadeDeleteEnabled )
				: voidFuture();
	}

	private static <T> CompletionStage<Void> cascadeLogicalOneToOneOrphanRemoval(
			final CascadingAction<T> action,
			final EventSource eventSource,
			final List<String> componentPath,
			final Object parent,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final String propertyName,
			final boolean isCascadeDeleteEnabled) throws HibernateException {

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
					final AttributeMapping propertyType = entry.getPersister().findAttributeMapping( componentPath.get( 0) );
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

					if ( valueEntry == null && isHibernateProxy( loadedValue ) ) {
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
							return voidFuture();
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

						if ( type.isAssociationType()
								&& ( (AssociationType) type ).getForeignKeyDirection().equals( TO_PARENT ) ) {
							// If FK direction is to-parent, we must remove the orphan *before* the queued update(s)
							// occur.  Otherwise, replacing the association on a managed entity, without manually
							// nulling and flushing, causes FK constraint violations.
							return ( (ReactiveSession) eventSource )
									.reactiveRemoveOrphanBeforeUpdates( entityName, loadedValue );
						}
						else {
							// Else, we must delete after the updates.
							return ( (ReactiveSession) eventSource )
									.reactiveRemove( entityName, loadedValue, isCascadeDeleteEnabled, DeleteContext.create() );
						}
					}
				}
			}
		}
		return voidFuture();
	}

	/**
	 * Check if the association is a one to one in the logical model (either a shared-pk
	 * or unique fk).
	 *
	 * @param type The type representing the attribute metadata
	 *
	 * @return True if the attribute represents a logical one to one association
	 */
	private static boolean isLogicalOneToOne(Type type) {
		return type.isEntityType() && ( (EntityType) type ).isLogicalOneToOne();
	}

	private static boolean cascadeAssociationNow(
			CascadingAction<?> action,
			CascadePoint cascadePoint,
			AssociationType associationType,
			SessionFactoryImplementor factory,
			boolean unownedTransient) {
		return associationType.getForeignKeyDirection().cascadeNow( cascadePoint )
				// For check on flush, we should only check unowned associations when strictness is enforced
				&& ( action != CascadingActions.CHECK_ON_FLUSH || unownedTransient || !isUnownedAssociation( associationType, factory ) );
	}

	private static boolean isUnownedAssociation(AssociationType associationType, SessionFactoryImplementor factory) {
		if ( associationType.isEntityType() ) {
			if ( associationType instanceof ManyToOneType ) {
				final ManyToOneType manyToOne = (ManyToOneType) associationType;
				// logical one-to-one + non-null unique key property name indicates unowned
				return manyToOne.isLogicalOneToOne() && manyToOne.getRHSUniqueKeyPropertyName() != null;
			}
			else if ( associationType instanceof OneToOneType ) {
				final OneToOneType oneToOne = (OneToOneType) associationType;
				// constrained false + non-null unique key property name indicates unowned
				return oneToOne.isNullable() && oneToOne.getRHSUniqueKeyPropertyName() != null;
			}
		}
		else if ( associationType.isCollectionType() ) {
			// for collections, we can ask the persister if we're on the inverse side
			return ( (CollectionType) associationType ).isInverse( factory );
		}
		return false;
	}

	private static <T> CompletionStage<Void>  cascadeComponent(
			final CascadingAction<T> action,
			final CascadePoint cascadePoint,
			final EventSource eventSource,
			final List<String> componentPath,
			final Object parent,
			final Object child,
			final CompositeType componentType,
			final T anything) {

		Object[] children = null;
		final Type[] types = componentType.getSubtypes();
		final String[] propertyNames = componentType.getPropertyNames();
		CompletionStage<Void> stage = voidFuture();
		for ( int i = 0; i < types.length; i++ ) {
			final CascadeStyle componentPropertyStyle = componentType.getCascadeStyle( i );
			final String subPropertyName = propertyNames[i];
			if ( componentPropertyStyle.doCascade( action.delegate() )
					|| componentPropertyStyle.hasOrphanDelete() && action.deleteOrphans() ) {
				if ( children == null ) {
					// Get children on demand.
					children = componentType.getPropertyValues( child, eventSource );
				}
				final Object propertyChild = children[i];
				final Type propertyType = types[i];
				stage = stage.thenCompose( v -> cascadeProperty(
						action,
						cascadePoint,
						eventSource,
						componentPath,
						parent,
						propertyChild,
						propertyType,
						componentPropertyStyle,
						subPropertyName,
						anything,
						false
					)
				);
			}
		}
		return stage;
	}

	private static <T> CompletionStage<Void> cascadeAssociation(
			final CascadingAction<T> action,
			final CascadePoint cascadePoint,
			final EventSource eventSource,
			final List<String> componentPath,
			final Object parent,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final T anything,
			final boolean isCascadeDeleteEnabled) {
		if ( type.isEntityType() || type.isAnyType() ) {
			return cascadeToOne( action, eventSource, parent, child, type, style, anything, isCascadeDeleteEnabled );
		}
		else if ( type.isCollectionType() ) {
			return cascadeCollection(
					action,
					cascadePoint,
					eventSource,
					componentPath,
					parent,
					child,
					style,
					anything,
					(CollectionType) type
			);
		}
		return voidFuture();
	}

	/**
	 * Cascade an action to a collection
	 */
	private static <T> CompletionStage<Void> cascadeCollection(
			final CascadingAction<T> action,
			final CascadePoint cascadePoint,
			final EventSource eventSource,
			final List<String> componentPath,
			final Object parent,
			final Object child,
			final CascadeStyle style,
			final T anything,
			final CollectionType type) {
		final CollectionPersister persister = eventSource.getFactory().getMappingMetamodel()
				.getCollectionDescriptor( type.getRole() );
		final Type elemType = persister.getElementType();
		//cascade to current collection elements
		if ( elemType.isEntityType() || elemType.isAnyType() || elemType.isComponentType() ) {
			return cascadeCollectionElements(
					action,
					cascadePoint == CascadePoint.AFTER_INSERT_BEFORE_DELETE
							? CascadePoint.AFTER_INSERT_BEFORE_DELETE_VIA_COLLECTION
							: cascadePoint,
					eventSource,
					componentPath,
					parent,
					child,
					type,
					style,
					elemType,
					anything,
					persister.isCascadeDeleteEnabled()

			);
		}
		return voidFuture();
	}

	/**
	 * Cascade an action to a to-one association or any type
	 */
	private static <T> CompletionStage<Void> cascadeToOne(
			final CascadingAction<T> action,
			final EventSource eventSource,
			final Object parent,
			final Object child,
			final Type type,
			final CascadeStyle style,
			final T anything,
			final boolean isCascadeDeleteEnabled) {
		final String entityName = type.isEntityType()
				? ( (EntityType) type ).getAssociatedEntityName()
				: null;
		if ( style.reallyDoCascade( action.delegate() ) ) {
			//not really necessary, but good for consistency...
			final PersistenceContext persistenceContext = eventSource.getPersistenceContextInternal();
			persistenceContext.addChildParent( child, parent );
			return voidFuture()
					.thenCompose( v -> action.cascade( eventSource, child, entityName, anything, isCascadeDeleteEnabled ) )
					.whenComplete( (v, e) -> persistenceContext.removeChildParent( child ) );
		}
		return voidFuture();
	}

	/**
	 * Cascade to the collection elements
	 */
	private static <T> CompletionStage<Void> cascadeCollectionElements(
			final CascadingAction<T> action,
			final CascadePoint cascadePoint,
			final EventSource eventSource,
			final List<String> componentPath,
			final Object parent,
			final Object child,
			final CollectionType collectionType,
			final CascadeStyle style,
			final Type elemType,
			final T anything,
			final boolean isCascadeDeleteEnabled) throws HibernateException {
		final boolean reallyDoCascade = style.reallyDoCascade( action.delegate() ) && child != CollectionType.UNFETCHED_COLLECTION;
		if ( reallyDoCascade ) {
			final boolean traceEnabled = LOG.isTraceEnabled();

			if ( traceEnabled ) {
				LOG.tracev( "Done cascade {0} for collection: {1}", action, collectionType.getRole() );
			}

			final Iterator<?> itr = action.getCascadableChildrenIterator( eventSource, collectionType, child );
			return loop( itr, (value, integer) -> cascadeProperty(
					action,
					cascadePoint,
					eventSource,
					componentPath,
					parent,
					value,
					elemType,
					style,
					collectionType.getRole().substring( collectionType.getRole().lastIndexOf( '.' ) + 1 ),
					anything,
					isCascadeDeleteEnabled
			) ).thenRun( () -> {
				if ( traceEnabled ) {
					LOG.tracev( "Done cascade {0} for collection: {1}", action, collectionType.getRole() );
				}
			} ).thenCompose( v -> doDeleteOrphans( action, eventSource, child, collectionType, style, elemType ) );
		}

		return doDeleteOrphans( action, eventSource, child, collectionType, style, elemType );
	}

	private static <T> CompletionStage<Void> doDeleteOrphans(
			CascadingAction<T> action,
			EventSource eventSource,
			Object child,
			CollectionType collectionType,
			CascadeStyle style,
			Type elemType) {
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
			return doDeleteOrphans( eventSource, entityName, (PersistentCollection<?>) child )
					.thenRun( () -> {
						if ( traceEnabled ) {
							LOG.tracev( "Done deleting orphans for collection: {0}", collectionType.getRole() );
						}
					} );
		}
		return voidFuture();
	}

	/**
	 * Delete any entities that were removed from the collection
	 */
	private static CompletionStage<Void> doDeleteOrphans(EventSource eventSource, String entityName, PersistentCollection<?> pc) {
		final Collection<?> orphans = getOrphans( eventSource, entityName, pc );
		final ReactiveSession session = (ReactiveSession) eventSource;
		return loop( orphans, Objects::nonNull, orphan -> {
			LOG.tracev( "Deleting orphaned entity instance: {0}", entityName );
			return session.reactiveRemove( entityName, orphan, false, DeleteContext.create() );
		} );
	}

	private static Collection<?> getOrphans(EventSource eventSource, String entityName, PersistentCollection<?> pc) {
		if ( pc.wasInitialized() ) {
			final CollectionEntry ce = eventSource.getPersistenceContextInternal().getCollectionEntry( pc );
			return ce == null ? EMPTY_LIST : ce.getOrphans( entityName, pc );
		}
		return pc.getQueuedOrphans( entityName );
	}
}
