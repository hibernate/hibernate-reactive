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
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.entity.EntityInitializer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingState;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchByUniqueKeyInitializer
 */
public class ReactiveEntitySelectFetchByUniqueKeyInitializer extends ReactiveEntitySelectFetchInitializer {

	private final ToOneAttributeMapping fetchedAttribute;

	public ReactiveEntitySelectFetchByUniqueKeyInitializer(
			FetchParentAccess parentAccess,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath fetchedNavigable,
			EntityPersister concreteDescriptor,
			DomainResultAssembler<?> keyAssembler) {
		super( parentAccess, fetchedAttribute, fetchedNavigable, concreteDescriptor, keyAssembler );
		this.fetchedAttribute = fetchedAttribute;
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(ReactiveRowProcessingState rowProcessingState) {
		if ( entityInstance != null || isInitialized ) {
			return voidFuture();
		}

		final EntityInitializer parentEntityInitializer = getParentEntityInitializer( parentAccess );
		if ( parentEntityInitializer != null && parentEntityInitializer.getEntityKey() != null ) {
			// make sure parentEntityInitializer.resolveInstance has been called before
			parentEntityInitializer.resolveInstance( rowProcessingState );
			if ( parentEntityInitializer.isEntityInitialized() ) {
				isInitialized = true;
				return voidFuture();
			}
		}

		if ( !isAttributeAssignableToConcreteDescriptor() ) {
			isInitialized = true;
			return voidFuture();
		}

		final Object entityIdentifier = keyAssembler.assemble( rowProcessingState );
		if ( entityIdentifier == null ) {
			isInitialized = true;
			return voidFuture();
		}
		final String entityName = concreteDescriptor.getEntityName();
		final String uniqueKeyPropertyName = fetchedAttribute.getReferencedPropertyName();
		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		final EntityUniqueKey euk = new EntityUniqueKey(
				entityName,
				uniqueKeyPropertyName,
				entityIdentifier,
				concreteDescriptor.getPropertyType( uniqueKeyPropertyName ),
				session.getFactory()
		);
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		setEntityInstance( persistenceContext.getEntity( euk ) );
		if ( entityInstance == null ) {
			final ReactiveEntitySelectFetchByUniqueKeyInitializer initializer = (ReactiveEntitySelectFetchByUniqueKeyInitializer) persistenceContext
					.getLoadContexts()
					.findInitializer( euk );
			if ( initializer == null ) {
				final JdbcValuesSourceProcessingState jdbcValuesSourceProcessingState = rowProcessingState.getJdbcValuesSourceProcessingState();
				jdbcValuesSourceProcessingState.registerInitializer( euk, this );

				return ( (ReactiveEntityPersister) concreteDescriptor )
						.reactiveLoadByUniqueKey( uniqueKeyPropertyName, entityIdentifier, session )
						.thenAccept( this::setEntityInstance )
						.thenAccept( v -> {
							// If the entity was not in the Persistence Context, but was found now,
							// add it to the Persistence Context
							if ( entityInstance != null ) {
								persistenceContext.addEntity( euk, entityInstance );
							}
							notifyResolutionListeners( entityInstance );
						} );
			}
			else {
				registerResolutionListener( this::setEntityInstance );
			}
		}
		if ( entityInstance != null ) {
			setEntityInstance( persistenceContext.proxyFor( entityInstance ) );
		}
		isInitialized = true;
		return voidFuture();
	}

	private EntityInitializer getParentEntityInitializer(FetchParentAccess parentAccess) {
		if ( parentAccess != null ) {
			return parentAccess.findFirstEntityInitializer();
		}
		return null;
	}

	private void setEntityInstance(Object instance) {
		entityInstance = instance;
	}
}
