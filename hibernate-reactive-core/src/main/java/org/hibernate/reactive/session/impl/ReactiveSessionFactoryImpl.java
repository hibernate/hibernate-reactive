/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;


import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.metamodel.spi.RuntimeMetamodelsImplementor;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionFactoryImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionFactoryImpl;



/**
 * A Hibernate {@link org.hibernate.SessionFactory} that can be
 * unwrapped to produce a {@link Stage.SessionFactory} or a
 * {@link Mutiny.SessionFactory}.
 */
public class ReactiveSessionFactoryImpl extends SessionFactoryImpl {

	public ReactiveSessionFactoryImpl(MetadataImplementor metadata, SessionFactoryOptions options) {
		super( metadata, options );

//		Map<Integer, Set<String>> contributions = getMetamodel().getTypeConfiguration()
//				.getJdbcToHibernateTypeContributionMap();
//
//		contributions.put( Types.JAVA_OBJECT, singleton( ObjectJavaType.class.getName() ) );
	}

	@Override
	public RuntimeMetamodelsImplementor getRuntimeMetamodels() {
		return super.getRuntimeMetamodels();
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
}
