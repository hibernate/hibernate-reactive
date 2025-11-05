/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.it.dirtychecking.Fruit;

import org.hibernate.testing.SqlStatementTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class DirtyCheckingIT extends BaseReactiveIT {

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( DirtyCheckingIT::updateQueryFilter, configuration.getProperties() );
		return configuration;
	}

	private static boolean updateQueryFilter(String s) {
		return s.toLowerCase().startsWith( "update " );
	}

	@BeforeEach
	public void clearTracker() {
		sqlTracker.clear();
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Fruit.class );
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		// There's only one test, so we don't need to clean the db.
		// This prevents extra queries in the log when the test is over.
		return voidFuture();
	}

	@Test
	public void testDirtyCheck(VertxTestContext context) {
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( s -> s.persist( new Fruit().setId( 5 ).setName( "Apple" ) ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s.find( Fruit.class, 5 ) ) )
						.invoke( fruit -> {
							assertThat( fruit ).hasFieldOrPropertyWithValue( "name", "Apple" );
							assertThat( sqlTracker.getLoggedQueries() )
									.as( "Dirty field detection failed, unexpected SQL mutation query" )
									.isEmpty();
						} )
		);
	}
}
