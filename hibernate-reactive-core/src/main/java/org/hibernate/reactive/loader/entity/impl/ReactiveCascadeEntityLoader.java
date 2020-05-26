/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.CascadingAction;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.JoinWalker;
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
