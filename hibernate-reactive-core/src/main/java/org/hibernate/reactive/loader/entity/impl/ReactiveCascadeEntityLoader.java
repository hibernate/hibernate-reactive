/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;


import org.hibernate.MappingException;
import org.hibernate.engine.spi.CascadingAction;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.entity.CascadeEntityJoinWalker;
import org.hibernate.persister.entity.OuterJoinLoadable;

/**
 * A reactive {@link org.hibernate.loader.entity.EntityLoader} that is
 * used for initializing unfetched associations during the
 * {@link org.hibernate.reactive.engine.impl.Cascade cascade} process.
 */
public class ReactiveCascadeEntityLoader extends ReactiveAbstractEntityLoader {

	public ReactiveCascadeEntityLoader(
			OuterJoinLoadable persister,
			CascadingAction action,
			SessionFactoryImplementor factory) throws MappingException {
		super(
				persister,
				persister.getIdentifierType(),
				factory,
				LoadQueryInfluencers.NONE
		);

		initFromWalker( new CascadeEntityJoinWalker( persister, action, factory ) );

		postInstantiate();

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Static select for action %s on entity %s: %s", action, entityName, getSQLString() );
		}
	}

}
