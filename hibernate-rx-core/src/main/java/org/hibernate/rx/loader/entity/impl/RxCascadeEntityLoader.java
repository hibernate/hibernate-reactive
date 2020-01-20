/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.*;
import org.hibernate.loader.JoinWalker;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.persister.entity.OuterJoinLoadable;

public class RxCascadeEntityLoader extends RxAbstractEntityLoader {

	public RxCascadeEntityLoader(
			OuterJoinLoadable persister,
			CascadingAction action,
			SessionFactoryImplementor factory) throws MappingException {
		super(
				persister,
				persister.getIdentifierType(),
				factory,
				LoadQueryInfluencers.NONE
		);

		JoinWalker walker = new CascadeEntityJoinWalker(
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

	public static UniqueEntityLoader create(OuterJoinLoadable persister, LockOptions lockOptions, SharedSessionContractImplementor session) {
		String internalProfile = session.getLoadQueryInfluencers().getInternalFetchProfile();
		if ( internalProfile==null || !LockMode.UPGRADE.greaterThan( lockOptions.getLockMode() ) ) {
			return null;
		}
		switch (internalProfile) {
			case "merge":
				return new RxCascadeEntityLoader( persister, CascadingActions.MERGE, session.getFactory() );
			case "refresh":
				return new RxCascadeEntityLoader( persister, CascadingActions.REFRESH, session.getFactory() );
			default:
				return null;
		}
	}
}
