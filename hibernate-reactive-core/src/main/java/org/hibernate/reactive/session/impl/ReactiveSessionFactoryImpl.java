/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.Filter;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.query.spi.QueryPlanCache;
import org.hibernate.engine.spi.SessionFactoryImplementor;
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
		// We aren't using lambdas or method reference because of a bug in the JVM:
		// https://bugs.openjdk.java.net/browse/JDK-8161588
		// Please, don't change this unless you've tested it with Quarkus
		super(metadata, options, new QueryPlanCache.QueryPlanCreator() {
			@Override
			public HQLQueryPlan createQueryPlan(String queryString, boolean shallow, Map<String, Filter> enabledFilters,
												SessionFactoryImplementor factory) {
				return new ReactiveHQLQueryPlan<>(queryString, shallow, enabledFilters, factory);
			}
		}); //TODO: pass ReactiveNativeHQLQueryPlan::new

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
