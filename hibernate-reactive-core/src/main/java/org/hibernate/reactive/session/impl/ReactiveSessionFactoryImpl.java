/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.reactive.boot.spi.ReactiveMetadataImplementor;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionFactoryImpl;
import org.hibernate.reactive.sql.exec.internal.ReactiveJdbcSelectWithActions;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionFactoryImpl;
import org.hibernate.sql.exec.spi.JdbcSelectWithActionsBuilder;

/**
 * A Hibernate {@link org.hibernate.SessionFactory} that can be
 * unwrapped to produce a {@link Stage.SessionFactory} or a
 * {@link Mutiny.SessionFactory}.
 */
public class ReactiveSessionFactoryImpl extends SessionFactoryImpl {

	public ReactiveSessionFactoryImpl(MetadataImplementor bootMetamodel, SessionFactoryOptions options, BootstrapContext bootstrapContext) {
		super( new ReactiveMetadataImplementor( bootMetamodel ), options, bootstrapContext );
	}

	@Override
	public QueryEngine getQueryEngine() {
		return super.getQueryEngine();
	}

	@Override
	public <T> T unwrap(Class<T> type) {
		if ( type.isAssignableFrom( Stage.SessionFactory.class ) ) {
			return type.cast( new StageSessionFactoryImpl( this ) );
		}
		if ( type.isAssignableFrom( Mutiny.SessionFactory.class ) ) {
			return type.cast( new MutinySessionFactoryImpl( this ) );
		}
		return super.unwrap( type );
	}

	public JdbcSelectWithActionsBuilder getJdbcSelectWithActionsBuilder(){
		return new ReactiveJdbcSelectWithActions.Builder();
	}

}
