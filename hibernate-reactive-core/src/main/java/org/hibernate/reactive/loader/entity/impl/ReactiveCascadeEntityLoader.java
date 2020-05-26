/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.CascadingAction;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.JoinWalker;
import org.hibernate.persister.entity.OuterJoinLoadable;

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

		JoinWalker walker = new ReactiveCascadeEntityJoinWalker(
				persister,
				action,
				factory
		);
		initFromWalker( walker );

		postInstantiate();

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Static select for action %s on entity %s: %s", action, entityName, getSQLString() );
		}
	}

}
