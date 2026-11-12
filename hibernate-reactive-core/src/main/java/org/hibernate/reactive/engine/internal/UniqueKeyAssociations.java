/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.AttributeMappingsList;
import org.hibernate.metamodel.mapping.CollectionPart;
import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.mapping.EmbeddableValuedModelPart;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.internal.ManyToManyCollectionPart;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.internal.ReactiveAbstractEntityPersister;
import org.hibernate.reactive.session.ReactiveQueryProducer;
import org.hibernate.reactive.util.internal.CompletionStages;

import static java.util.Collections.emptyIterator;
import static org.hibernate.Hibernate.isInitialized;
import static org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer.UNFETCHED_PROPERTY;
import static org.hibernate.metamodel.mapping.ForeignKeyDescriptor.Nature.KEY;
import static org.hibernate.reactive.util.internal.CompletionStages.loop;
import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * Fetches the unfetched proxies of associations that reference a unique key of the
 * associated entity, for example a natural id, instead of its primary key.
 * <p>
 * Hibernate needs the value of the unique key to check if the association is dirty and
 * to write the foreign key column. Hibernate ORM initializes the proxy transparently when
 * it needs the value, but Hibernate Reactive can't, so we fetch these proxies before they
 * are needed: at the beginning of a flush, and before an insert or an update.
 * Associations referencing the primary key don't require a fetch.
 *
 * @see org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping#lazyInitialize(Object)
 */
public final class UniqueKeyAssociations {

	private UniqueKeyAssociations() {
	}

	/**
	 * Fetch the unfetched proxies referenced by the given entity, including the ones in its
	 * embeddables and in the elements of its collections.
	 */
	public static CompletionStage<Void> fetchUnfetchedProxies(
			EntityPersister persister,
			Object entity,
			SharedSessionContractImplementor session) {
		return hasUniqueKeyAssociations( persister )
				? fetchUnfetchedProxies( persister.getAttributeMappings(), persister.getValues( entity ), session )
				: voidFuture();
	}

	/**
	 * Fetch the unfetched proxies in the given state of an entity, including the ones in its
	 * embeddables and in the elements of its collections.
	 */
	public static CompletionStage<Void> fetchUnfetchedProxies(
			EntityPersister persister,
			Object[] values,
			SharedSessionContractImplementor session) {
		return hasUniqueKeyAssociations( persister )
				? fetchUnfetchedProxies( persister.getAttributeMappings(), values, session )
				: voidFuture();
	}

	private static boolean hasUniqueKeyAssociations(EntityPersister persister) {
		return persister instanceof ReactiveAbstractEntityPersister reactivePersister
				? reactivePersister.hasUniqueKeyAssociations()
				: hasUniqueKeyAssociations( persister.getAttributeMappings() );
	}

	/**
	 * Whether the attributes include associations referencing a unique key, including the ones
	 * in embeddables and collections.
	 *
	 * @see ReactiveAbstractEntityPersister#hasUniqueKeyAssociations()
	 */
	public static boolean hasUniqueKeyAssociations(AttributeMappingsList attributeMappings) {
		for ( int i = 0; i < attributeMappings.size(); i++ ) {
			if ( hasUniqueKeyAssociations( attributeMappings.get( i ) ) ) {
				return true;
			}
		}
		return false;
	}

	// Same conditions used by Hibernate ORM to decide if it needs to initialize the proxy,
	// see ToOneAttributeMapping#extractValue and SimpleForeignKeyDescriptor#getAssociationKeyFromSide
	private static boolean hasUniqueKeyAssociations(ModelPart part) {
		if ( part instanceof ToOneAttributeMapping toOne ) {
			return toOne.getReferencedPropertyName() != null && toOne.getSideNature() == KEY;
		}
		else if ( part instanceof ManyToManyCollectionPart manyToMany ) {
			return !manyToMany.isReferenceToPrimaryKey();
		}
		else if ( part instanceof EmbeddableValuedModelPart embeddable ) {
			return hasUniqueKeyAssociations( embeddable.getEmbeddableTypeDescriptor().getAttributeMappings() );
		}
		else if ( part instanceof PluralAttributeMapping plural ) {
			// Hibernate doesn't write the rows of an inverse collection
			return !plural.getCollectionDescriptor().isInverse()
					&& ( hasUniqueKeyAssociations( plural.getElementDescriptor() )
						|| plural.getIndexDescriptor() != null && hasUniqueKeyAssociations( plural.getIndexDescriptor() ) );
		}
		return false;
	}

	private static CompletionStage<Void> fetchUnfetchedProxies(
			AttributeMappingsList attributeMappings,
			Object[] values,
			SharedSessionContractImplementor session) {
		return loop(
				0, attributeMappings.size(),
				index -> hasUniqueKeyAssociations( attributeMappings.get( index ) ),
				index -> fetchUnfetchedProxies( attributeMappings.get( index ), values[index], session )
		);
	}

	private static CompletionStage<Void> fetchUnfetchedProxies(
			ModelPart part,
			Object value,
			SharedSessionContractImplementor session) {
		if ( value == null || value == UNFETCHED_PROPERTY ) {
			return voidFuture();
		}
		else if ( part instanceof ToOneAttributeMapping || part instanceof ManyToManyCollectionPart ) {
			return isInitialized( value )
					? voidFuture()
					: ( (ReactiveQueryProducer) session ).reactiveFetch( value, false ).thenCompose( CompletionStages::voidFuture );
		}
		else if ( part instanceof EmbeddableValuedModelPart embeddable ) {
			final EmbeddableMappingType embeddableType = embeddable.getEmbeddableTypeDescriptor();
			return fetchUnfetchedProxies( embeddableType.getAttributeMappings(), embeddableType.getValues( value ), session );
		}
		else if ( part instanceof PluralAttributeMapping plural ) {
			return fetchUnfetchedProxies( plural, value, session );
		}
		return voidFuture();
	}

	private static CompletionStage<Void> fetchUnfetchedProxies(
			PluralAttributeMapping plural,
			Object collection,
			SharedSessionContractImplementor session) {
		if ( collection instanceof PersistentCollection<?> persistentCollection && !persistentCollection.wasInitialized() ) {
			// There are no new elements to write
			return voidFuture();
		}
		final CollectionPart element = plural.getElementDescriptor();
		final CollectionPart index = plural.getIndexDescriptor();
		final CompletionStage<Void> elementsStage = hasUniqueKeyAssociations( element )
				? loop(
						elements( collection ),
						(value, i) -> fetchUnfetchedProxies( element, value, session )
				)
				: voidFuture();
		return collection instanceof Map<?, ?> map && index != null && hasUniqueKeyAssociations( index )
				? elementsStage.thenCompose( v -> loop(
						map.keySet().iterator(),
						(key, i) -> fetchUnfetchedProxies( index, key, session )
				) )
				: elementsStage;
	}

	private static Iterator<?> elements(Object collection) {
		if ( collection instanceof Map<?, ?> map ) {
			return map.values().iterator();
		}
		else if ( collection instanceof Collection<?> elements ) {
			return elements.iterator();
		}
		else if ( collection instanceof Object[] array ) {
			return Arrays.asList( array ).iterator();
		}
		return emptyIterator();
	}
}
