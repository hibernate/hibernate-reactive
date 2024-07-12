/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializer;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchByUniqueKeyInitializer
 */
public class ReactiveEntitySelectFetchByUniqueKeyInitializer
		extends ReactiveEntitySelectFetchInitializer<EntitySelectFetchInitializer.EntitySelectFetchInitializerData> {

	private final ToOneAttributeMapping fetchedAttribute;

	public ReactiveEntitySelectFetchByUniqueKeyInitializer(
			InitializerParent<?> parent,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath fetchedNavigable,
			EntityPersister concreteDescriptor,
			DomainResult<?> keyResult,
			boolean affectedByFilter,
			AssemblerCreationState creationState) {
		super(
				parent,
				fetchedAttribute,
				fetchedNavigable,
				concreteDescriptor,
				keyResult,
				affectedByFilter,
				creationState
		);
		this.fetchedAttribute = fetchedAttribute;
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(EntitySelectFetchInitializerData actual) {
		final ReactiveEntitySelectFetchInitializerData data = (ReactiveEntitySelectFetchInitializerData) actual;
		final String entityName = concreteDescriptor.getEntityName();
		final String uniqueKeyPropertyName = fetchedAttribute.getReferencedPropertyName();

		final SharedSessionContractImplementor session = data.getRowProcessingState().getSession();

		final EntityUniqueKey euk = new EntityUniqueKey(
				entityName,
				uniqueKeyPropertyName,
				data.getEntityIdentifier(),
				concreteDescriptor.getPropertyType( uniqueKeyPropertyName ),
				session.getFactory()
		);
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		data.setInstance( persistenceContext.getEntity( euk ) );
		if ( data.getInstance() == null ) {
			return ( (ReactiveEntityPersister) concreteDescriptor )
					.reactiveLoadByUniqueKey( uniqueKeyPropertyName, data.getEntityIdentifier(), session )
					.thenAccept( data::setInstance )
					.thenAccept( v -> {
						// If the entity was not in the Persistence Context, but was found now,
						// add it to the Persistence Context
						if ( data.getInstance() != null ) {
							persistenceContext.addEntity( euk, data.getInstance() );
						}
					} );
		}
		if ( data.getInstance() != null ) {
			data.setInstance( persistenceContext.proxyFor( data.getInstance() ) );
		}
		return voidFuture();
	}
}
