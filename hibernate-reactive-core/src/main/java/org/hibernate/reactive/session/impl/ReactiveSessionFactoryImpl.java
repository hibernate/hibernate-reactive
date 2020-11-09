/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionFactoryImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionFactoryImpl;
import org.hibernate.type.LocalDateTimeType;
import org.hibernate.type.LocalDateType;
import org.hibernate.type.LocalTimeType;
import org.hibernate.type.OffsetDateTimeType;

import java.sql.Types;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * A Hibernate {@link org.hibernate.SessionFactory} that can be
 * unwrapped to produce a {@link Stage.SessionFactory} or a
 * {@link Mutiny.SessionFactory}.
 */
public class ReactiveSessionFactoryImpl extends SessionFactoryImpl {
	public ReactiveSessionFactoryImpl(MetadataImplementor metadata, SessionFactoryOptions options) {
		super( metadata, options, ReactiveHQLQueryPlan::new ); //TODO: pass ReactiveNativeHQLQueryPlan::new

		Map<Integer, Set<String>> contributions =
				getMetamodel().getTypeConfiguration().getJdbcToHibernateTypeContributionMap();
		//override the default type mappings for temporal types to return java.time instead of java.sql
		contributions.put( Types.TIMESTAMP_WITH_TIMEZONE, singleton( OffsetDateTimeType.class.getName() ) );
		contributions.put( Types.TIMESTAMP, singleton( LocalDateTimeType.class.getName() ) );
		contributions.put( Types.TIME, singleton( LocalTimeType.class.getName() ) );
		contributions.put( Types.DATE, singleton( LocalDateType.class.getName() ) );
		contributions.put( Types.JAVA_OBJECT, singleton( ObjectType.class.getName() ) );
	}

	@Override
	public <T> T unwrap(Class<T> type) {
		if ( type.isAssignableFrom(Stage.SessionFactory.class) ) {
			return type.cast( new StageSessionFactoryImpl( this ) );
		}
		if ( type.isAssignableFrom(Mutiny.SessionFactory.class) ) {
			return type.cast( new MutinySessionFactoryImpl( this ) );
		}
		return super.unwrap(type);
	}
}
