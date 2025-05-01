/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.HibernateException;
import org.hibernate.collection.spi.AbstractPersistentCollection;
import org.hibernate.collection.spi.PersistentArrayHolder;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.CollectionEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.type.ArrayType;
import org.hibernate.type.CollectionType;
import org.hibernate.type.CustomCollectionType;
import org.hibernate.type.EntityType;
import org.hibernate.type.ForeignKeyDirection;
import org.hibernate.type.MapType;
import org.hibernate.type.Type;

import static org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer.UNFETCHED_PROPERTY;
import static org.hibernate.internal.util.collections.CollectionHelper.mapOfSize;
import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Reactive operations that really belong to {@link CollectionType}
 *
 */
public class CollectionTypes {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * @see  org.hibernate.type.AbstractType#replace(Object, Object, SharedSessionContractImplementor, Object, Map, ForeignKeyDirection)
	 */
	public static CompletionStage<Object> replace(
			CollectionType type,
			Object original,
			Object target,
			SharedSessionContractImplementor session,
			Object owner,
			Map<Object, Object> copyCache,
			ForeignKeyDirection foreignKeyDirection)
			throws HibernateException {
		// Collection and OneToOne are the only associations that could be TO_PARENT
		return type.getForeignKeyDirection() == foreignKeyDirection
				? replace( type, original, target, session, owner, copyCache )
				: completedFuture( target );
	}

	/**
	 * @see CollectionType#replace(Object, Object, SharedSessionContractImplementor, Object, Map)
	 */
	public static CompletionStage<Object> replace(
			CollectionType type,
			Object original,
			Object target,
			SharedSessionContractImplementor session,
			Object owner,
			Map<Object, Object> copyCache) throws HibernateException {
		if ( original == null ) {
			return completedFuture( replaceNullOriginal( target, session ) );
		}
		else if ( !Hibernate.isInitialized( original ) ) {
			return completedFuture( replaceUninitializedOriginal( type, original, target, session, copyCache ) );
		}
		else {
			return replaceOriginal( type, original, target, session, owner, copyCache );
		}
	}

	// todo: make org.hibernate.type.CollectionType#replaceNullOriginal public ?
	/**
	 * @see CollectionType#replaceNullOriginal(Object, SharedSessionContractImplementor)
	 */
	private static Object replaceNullOriginal(
			Object target,
			SharedSessionContractImplementor session) {
		if ( target == null ) {
			return null;
		}
		else if ( target instanceof Collection<?> collection ) {
			collection.clear();
			return collection;
		}
		else if ( target instanceof Map<?, ?> map ) {
			map.clear();
			return map;
		}
		else {
			final PersistenceContext persistenceContext = session.getPersistenceContext();
            if ( persistenceContext.getCollectionHolder( target )
					instanceof PersistentArrayHolder<?> arrayHolder ) {
				persistenceContext.removeCollectionHolder( target );
				arrayHolder.beginRead();
				final PluralAttributeMapping attributeMapping =
						persistenceContext.getCollectionEntry( arrayHolder )
								.getLoadedPersister().getAttributeMapping();
				arrayHolder.injectLoadedState( attributeMapping, null );
				arrayHolder.endRead();
				arrayHolder.dirty();
				persistenceContext.addCollectionHolder( arrayHolder );
				return arrayHolder.getArray();
			}
		}
		return null;
	}

	// todo: make org.hibernate.type.CollectionType#replaceUninitializedOriginal public
	private static Object replaceUninitializedOriginal(
			CollectionType type,
			Object original,
			Object target,
			SharedSessionContractImplementor session,
			Map<Object, Object> copyCache) {
		final PersistentCollection<?> persistentCollection = (PersistentCollection<?>) original;
		if ( persistentCollection.hasQueuedOperations() ) {
			if ( original == target ) {
				// A managed entity with an uninitialized collection is being merged,
				// We need to replace any detached entities in the queued operations
				// with managed copies.
				final AbstractPersistentCollection<?> collection =
						(AbstractPersistentCollection<?>) original;
				collection.replaceQueuedOperationValues(
						session.getFactory()
								.getMappingMetamodel()
								.getCollectionDescriptor( type.getRole() ), copyCache
				);
			}
			else {
				// original is a detached copy of the collection;
				// it contains queued operations, which will be ignored
				LOG.ignoreQueuedOperationsOnMerge(
						collectionInfoString( type.getRole(), persistentCollection.getKey() ) );
			}
		}
		return target;
	}

	/**
	 * @see CollectionType#replaceOriginal(Object, Object, SharedSessionContractImplementor, Object, Map)
	 */
	private static CompletionStage<Object> replaceOriginal(
			CollectionType type,
			Object original,
			Object target,
			SharedSessionContractImplementor session,
			Object owner,
			Map<Object, Object> copyCache) {

		//for arrays, replaceElements() may return a different reference, since
		//the array length might not match
		return replaceElements(
				type,
				original,
				instantiateResultIfNecessary( type, original, target ),
				owner,
				copyCache,
				session
		).thenCompose( result -> {
			if ( original == target ) {
				// get the elements back into the target making sure to handle dirty flag
				//TODO: this is a little inefficient, don't need to do a whole
				//      deep replaceElements() call
				final CompletionStage<Object> replaced =
						replaceElements( type, result, target, owner, copyCache, session );
				if ( target instanceof PersistentCollection<?> collection
						&& !collection.isDirty() ) {
					return replaced.thenApply( unused -> {
						collection.clearDirty();
						return target;
					} );
				}
				else {
					return replaced.thenApply( unused -> target );
				}
			}
			else {
				return completedFuture( result );
			}
		} );
	}

	/**
	 * @see CollectionType#replaceElements(Object, Object, Object, Map, SharedSessionContractImplementor)
	 */
	private static CompletionStage<Object> replaceElements(
			CollectionType type,
			Object original,
			Object target,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		if ( type instanceof ArrayType ) {
			return replaceArrayTypeElements( type, original, target, owner, copyCache, session );
		}
		else if ( type instanceof CustomCollectionType ) {
			return completedFuture( type.replaceElements( original, target, owner, copyCache, session ) );
		}
		else if ( type instanceof MapType ) {
			return replaceMapTypeElements(
					type,
					(Map<?, ?>) original,
					(Map<Object, Object>) target,
					owner,
					copyCache,
					session
			);
		}
		else {
			return replaceCollectionTypeElements(
					type,
					original,
					(Collection<Object>) target,
					owner,
					copyCache,
					session
			);
		}
	}

	private static CompletionStage<Object> replaceCollectionTypeElements(
			CollectionType type,
			Object original,
			final Collection<Object> result,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		result.clear();
		// copy elements into newly empty target collection
		final Type elemType = type.getElementType( session.getFactory() );
        return loop( (Collection<?>) original,
				o -> getReplace( elemType, o, owner, session, copyCache )
						.thenAccept( result::add ) )
				.thenCompose( v -> preserveSnapshotIfNecessary( original, result, owner, copyCache, session, elemType ) );
	}

	private static CompletionStage<Object> preserveSnapshotIfNecessary(
			Object original,
			Collection<Object> result,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session,
			Type elemType) {
		// if the original is a PersistentCollection, and that original
		// was not flagged as dirty, then reset the target's dirty flag
		// here after the copy operation.
		// </p>
		// One thing to be careful of here is a "bare" original collection
		// in which case we should never ever ever reset the dirty flag
		// on the target because we simply do not know...
		if ( original instanceof PersistentCollection<?> originalCollection
				&& result instanceof PersistentCollection<?> resultCollection ) {
			return preserveSnapshot(
					originalCollection, resultCollection,
					elemType, owner, copyCache, session
			).thenApply( v -> {
				if ( !originalCollection.isDirty() ) {
					resultCollection.clearDirty();
				}
				return result;
			} );
		}
		else {
			return completedFuture( result );
		}
	}

	private static CompletionStage<Object> replaceMapTypeElements(
			CollectionType type,
			Map<?, ?> original,
			Map<Object, Object> target,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		final CollectionPersister persister =
				session.getFactory().getRuntimeMetamodels().getMappingMetamodel()
						.getCollectionDescriptor( type.getRole() );
        target.clear();
		return loop(
				original.entrySet(), entry -> {
					final Map.Entry<?, ?> me = entry;
					return getReplace( persister.getIndexType(), me.getKey(), owner, session, copyCache )
							.thenCompose( key -> getReplace(
												  persister.getElementType(),
												  me.getValue(),
												  owner,
												  session,
												  copyCache
										  ).thenAccept( value -> target.put( key, value ) )
							);
				}
		).thenApply( unused -> target);
	}

	private static CompletionStage<Object> replaceArrayTypeElements(
			CollectionType type,
			Object original,
			Object target,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		final Object result;
		final int length = Array.getLength( original );
		if ( length != Array.getLength( target ) ) {
			//note: this affects the return value!
			result = ( (ArrayType) type ).instantiateResult( original );
		}
		else {
			result = target;
		}

		final Type elemType = type.getElementType( session.getFactory() );
		return loop( 0, length,
				i -> getReplace( elemType, Array.get( original, i ), owner, session, copyCache )
						.thenApply( o -> {
							Array.set( result, i, o );
							return result;
						} )
		).thenApply( unused -> result );
	}

	private static CompletionStage<Object> getReplace(
			Type elemType,
			Object o,
			Object owner,
			SharedSessionContractImplementor session,
			Map<Object, Object> copyCache) {
		return getReplace( elemType, o, null, owner, session, copyCache );
	}

	private static CompletionStage<Object> getReplace(
			Type elemType,
			Object object,
			Object target,
			Object owner,
			SharedSessionContractImplementor session,
			Map<Object, Object> copyCache) {
        return elemType instanceof EntityType entityType
				? EntityTypes.replace( entityType, object, target, session, owner, copyCache )
				: completedFuture( elemType.replace( object, target, session, owner, copyCache) );
	}

	/**
	 * @see CollectionType#preserveSnapshot(PersistentCollection, PersistentCollection, Type, Object, Map, SharedSessionContractImplementor)
	 */
	private static CompletionStage<Void> preserveSnapshot(
			PersistentCollection<?> original,
			PersistentCollection<?> result,
			Type elemType,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		final CollectionEntry ce = session.getPersistenceContextInternal().getCollectionEntry( result );
		if ( ce != null ) {
			return createSnapshot( original, result, elemType, owner, copyCache, session )
					.thenAccept( serializable -> ce.resetStoredSnapshot( result, serializable ) );
		}
		else {
			return voidFuture();
		}
	}

	/**
	 * @see CollectionType#createSnapshot(PersistentCollection, PersistentCollection, Type, Object, Map, SharedSessionContractImplementor)
	 */
	private static CompletionStage<Serializable> createSnapshot(
			PersistentCollection<?> original,
			PersistentCollection<?> result,
			Type elemType,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		final Serializable originalSnapshot = original.getStoredSnapshot();
		if ( originalSnapshot instanceof List<?> list ) {
			return createListSnapshot( list, elemType, owner, copyCache, session );
		}
		else if ( originalSnapshot instanceof Map<?, ?> map ) {
			return createMapSnapshot( map, result, elemType, owner, copyCache, session );
		}
		else if ( originalSnapshot instanceof Object[] array ) {
			return createArraySnapshot( array, elemType, owner, copyCache, session );
		}
		else {
			// retain the same snapshot
			return completedFuture( result.getStoredSnapshot() );
		}
	}

	/**
	 * @see CollectionType#createArraySnapshot(Object[], Type, Object, Map, SharedSessionContractImplementor)
	 */
	private static CompletionStage<Serializable> createArraySnapshot(
			Object[] array,
			Type elemType,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		return loop( 0, array.length,
				i -> getReplace( elemType, array[i], owner, session, copyCache )
						.thenAccept( o -> array[i] = o )
		).thenApply( unused -> array );
	}

	/**
	 * @see CollectionType#createMapSnapshot(Map, PersistentCollection, Type, Object, Map, SharedSessionContractImplementor)
	 */
	private static <K,V> CompletionStage<Serializable> createMapSnapshot(
			Map<K,V> map,
			PersistentCollection<?> result,
			Type elemType,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		final Map<?, ?> resultSnapshot = (Map<?, ?>) result.getStoredSnapshot();
		final Map<K, V> targetMap =
				map instanceof SortedMap<K, V> sortedMap
						? new TreeMap<>( sortedMap.comparator() )
						: mapOfSize(map.size());
        return loop( map.entrySet(),
				entry -> getReplace( elemType, entry.getValue(), resultSnapshot, owner, session, copyCache )
						.thenAccept( newValue -> {
							final K key = entry.getKey();
							final V value = entry.getValue();
							//noinspection unchecked
							targetMap.put( key == value ? (K) newValue : key, (V) newValue );
						} )
		).thenApply( v -> (Serializable) targetMap );
	}

	/**
	 * @see CollectionType#createListSnapshot(List, Type, Object, Map, SharedSessionContractImplementor)
	 */
	private static CompletionStage<Serializable> createListSnapshot(
			List<?> list,
			Type elemType,
			Object owner,
			Map<Object, Object> copyCache,
			SharedSessionContractImplementor session) {
		final ArrayList<Object> targetList = new ArrayList<>( list.size() );
		return loop( list,
				obj -> getReplace( elemType, obj, owner, session, copyCache )
						.thenAccept( targetList::add )
		).thenApply( unused -> targetList );
	}

	/**
	 * @see CollectionType#instantiateResultIfNecessary(Object, Object)
	 */
	private static Object instantiateResultIfNecessary(CollectionType type, Object original, Object target) {
		// for a null target, or a target which is the same as the original,
		// we need to put the merged elements in a new collection
		// by default just use an unanticipated capacity since we don't
		// know how to extract the capacity to use from original here...
		return target == null
			|| target == original
			|| target == UNFETCHED_PROPERTY
			|| target instanceof PersistentCollection<?> collection && collection.isWrapper( original )
				? type.instantiate( -1 )
				: target;
	}
}
