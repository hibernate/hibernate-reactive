/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Test the stateless session actually execute the operations in batch.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class ReactiveStatelessWithBatchTest extends BaseReactiveTest {
	private static SqlStatementTracker sqlTracker;

	private static final Object[] PIGS = {
			new GuineaPig( 11, "One" ),
			new GuineaPig( 22, "Two" ),
			new GuineaPig( 33, "Three" ),
			new GuineaPig( 44, "Four" ),
			new GuineaPig( 55, "Five" ),
			new GuineaPig( 66, "Six" )
	};

	private static final Object[] PIGS_AFTER_DELETE = List.of( PIGS )
			.subList( 2, PIGS.length )
			.toArray();

	private static final Object[] PIGS_AFTER_UPDATE = {
			new GuineaPig( 11, "One updated" ),
			new GuineaPig( 22, "Two updated" ),
			new GuineaPig( 33, "Three" ),
			new GuineaPig( 44, "Four" ),
			new GuineaPig( 55, "Five" ),
			new GuineaPig( 66, "Six" )
	};

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( GuineaPig.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker(
				ReactiveStatelessWithBatchTest::filter,
				configuration.getProperties()
		);
		return configuration;
	}

	@BeforeEach
	public void clearTracker() {
		sqlTracker.clear();
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean filter(String s) {
		String[] accepted = { "merge ", "insert ", "update ", "delete " };
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void testMutinyMergeUpsertAll(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.upsertAll( PIGS ) )
				.invoke( () -> assertSqlLogTracker( "merge into pig as t using (.*)" ) )
				.chain( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS ) ) )
		);
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void testMutinyMergeUpsertAllWithBatchSize(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.upsertAll( 10, PIGS ) )
				.invoke( () -> assertSqlLogTracker( "merge into pig as t using (.*)" ) )
				.chain( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS ) ) )
		);
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void testMutinyMergeUpsertMultiple(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.upsertMultiple( List.of( PIGS ) ) )
				.invoke( () -> assertSqlLogTracker( "merge into pig as t using (.*)" ) )
				.chain( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS ) ) )
		);
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void testStageMergeUpsertAll(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.upsertAll( PIGS ) )
				.thenRun( () -> assertSqlLogTracker( "merge into pig as t using (.*)" ) )
				.thenCompose( v -> assertExpectedResult( PIGS ) )
		);
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void testStageMergeUpsertAllWithBatchSize(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.upsertAll( 10, PIGS ) )
				.thenRun(() -> assertSqlLogTracker( "merge into pig as t using (.*)" ) )
				.thenCompose( v -> assertExpectedResult( PIGS ) )
		);
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void testStageMergeUpsertMultiple(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.upsertMultiple( List.of( PIGS ) ) )
				.thenRun( () -> assertSqlLogTracker( "merge into pig as t using (.*)" ) )
				.thenCompose( v -> assertExpectedResult( PIGS ) )
		);
	}

	@Test
	public void testMutinyBatchingInsert(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( () -> assertSqlLogTracker( "insert into pig \\(name,id\\) values (.*)" ) )
				.chain( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS ) ) )
		);
	}

	@Test
	public void testMutinyBatchingInsertMultiple(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertMultiple( List.of( PIGS ) ) )
				.invoke( () -> assertSqlLogTracker( "insert into pig \\(name,id\\) values (.*)" ) )
				.chain( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS ) ) )
		);
	}

	@Test
	public void testMutinyBatchingInsertAllNoBatchSizeParameter(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( PIGS ) )
				.invoke( () -> assertSqlLogTracker( "insert into pig \\(name,id\\) values (.*)" ) )
				.chain( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS ) ) )
		);
	}

	@Test
	public void testStageBatchingInsert(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> assertSqlLogTracker( "insert into pig \\(name,id\\) values (.*)" ) )
				.thenCompose( v -> assertExpectedResult( PIGS ) )
		);
	}

	@Test
	public void testStageBatchingInsertMultiple(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insertMultiple( List.of( PIGS ) ) )
				.thenAccept( v -> assertSqlLogTracker( "insert into pig \\(name,id\\) values (.*)" ) )
				.thenCompose( v -> assertExpectedResult( PIGS ) )
		);
	}

	@Test
	public void testStageBatchingInsertNoBatchSizeParameter(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( PIGS ) )
				.thenAccept( v -> assertSqlLogTracker( "insert into pig \\(name,id\\) values (.*)" ) )
				.thenCompose( v -> assertExpectedResult( PIGS ) )
		);
	}

	@Test
	public void testMutinyBatchingDelete(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p", GuineaPig.class ).getResultList()
				) )
				.chain( pigs -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.deleteAll( 10, pigs.subList( 0, 2 ).toArray() )
				) )
				.invoke( () -> assertSqlLogTracker( "delete from pig where id=.*" ) )
				.call( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS_AFTER_DELETE ) ) )
		);
	}

	@Test
	public void testMutinyBatchingDeleteMultiple(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p", GuineaPig.class ).getResultList()
				) )
				.chain( pigs -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.deleteMultiple( pigs.subList( 0, 2 ) ) )
				)
				.invoke( () -> assertSqlLogTracker( "delete from pig where id=.*" ) )
				.call( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS_AFTER_DELETE ) ) )
		);
	}

	@Test
	public void testMutinyBatchingDeleteAllNoBatchSizeParameter(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p", GuineaPig.class ).getResultList()
				) )
				.chain( pigs -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.deleteAll( pigs.subList( 0, 2 ).toArray() )
				) )
				.invoke( () -> assertSqlLogTracker( "delete from pig where id=.*" ) )
				.call( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS_AFTER_DELETE ) ) )
		);
	}

	@Test
	public void testStageBatchingDelete(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenRun( sqlTracker::clear )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p", GuineaPig.class ).getResultList()
						.thenCompose( pigs -> s.delete( 10, pigs.subList( 0, 2 ).toArray() ) )
				) )
				.thenAccept( v -> assertSqlLogTracker( "delete from pig where id=.*" ) )
				.thenCompose( v -> assertExpectedResult( PIGS_AFTER_DELETE ) )
		);
	}

	@Test
	public void testStageBatchingDeleteMultiple(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenRun( sqlTracker::clear )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p", GuineaPig.class ).getResultList()
						.thenCompose( pigs -> s.deleteMultiple( pigs.subList( 0, 2 ) ) )
				) )
				.thenAccept( v -> assertSqlLogTracker( "delete from pig where id=.*" ) )
				.thenCompose( v -> assertExpectedResult( PIGS_AFTER_DELETE ) )
		);
	}

	@Test
	public void testStageBatchingDeleteNoBatchSizeParameter(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenRun( sqlTracker::clear )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p", GuineaPig.class ).getResultList()
						.thenCompose( pigs -> s.delete( pigs.subList( 0, 2 ).toArray() ) )
				) )
				.thenAccept( v -> assertSqlLogTracker( "delete from pig where id=.*" ) )
				.thenCompose( v -> assertExpectedResult( PIGS_AFTER_DELETE ) )
		);
	}

	@Test
	public void testMutinyBatchingUpdate(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.chain( pigs -> {
							pigs.get( 0 ).setName( "One updated" );
							pigs.get( 1 ).setName( "Two updated" );
							return s.updateAll( 10, pigs.toArray() );
						} )
				) )
				.invoke( () -> assertSqlLogTracker( "update pig set name=.* where id=.*" ) )
				.call( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS_AFTER_UPDATE ) ) )
		);
	}

	@Test
	public void testMutinyBatchingUpdateMultiple(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.chain( pigs -> {
							pigs.get( 0 ).setName( "One updated" );
							pigs.get( 1 ).setName( "Two updated" );
							return s.updateMultiple( pigs.subList( 0, 2 ) );
						} ) )
				)
				.invoke( () -> assertSqlLogTracker( "update pig set name=.* where id=.*" ) )
				.call( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS_AFTER_UPDATE ) ) )
		);
	}

	@Test
	public void testMutinyBatchingUpdateAllNoBatchSizeParameter(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "select p from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.chain( pigs -> {
							pigs.get( 0 ).setName( "One updated" );
							pigs.get( 1 ).setName( "Two updated" );
							return s.updateAll( pigs.toArray() );
						} ) )
				)
				.invoke( () -> assertSqlLogTracker( "update pig set name=.* where id=.*" ) )
				.call( () -> Uni.createFrom().completionStage( assertExpectedResult( PIGS_AFTER_UPDATE ) ) )
		);
	}

	@Test
	public void testStageBatchingUpdate(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> sqlTracker.clear() )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.thenApply( pigs -> {
							pigs.get( 0 ).setName( "One updated" );
							pigs.get( 1 ).setName( "Two updated" );
							return s.update( 10, pigs.toArray() );
						} )
				) )
				.thenAccept( v -> assertSqlLogTracker( "update pig set name=.* where id=.*" ) )
				.thenCompose( v -> assertExpectedResult( PIGS_AFTER_UPDATE ) )
		);
	}

	@Test
	public void testStageBatchingUpdateMultiple(VertxTestContext context) {
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenRun( sqlTracker::clear )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.thenApply( pigs -> {
							pigs.get( 0 ).setName( "One updated" );
							pigs.get( 1 ).setName( "Two updated" );
							return s.updateMultiple( pigs );
						} )
				) )
				.thenAccept( v -> assertSqlLogTracker( "update pig set name=.* where id=.*" ) )
				.thenCompose( v -> assertExpectedResult( PIGS_AFTER_UPDATE ) )
		);
	}

	@Test
	public void testStageBatchingUpdateNoBatchSizeParameter(VertxTestContext context) {
		test(context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenRun( sqlTracker::clear )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.createQuery( "from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.thenApply( pigs -> {
							pigs.get( 0 ).setName( "One updated" );
							pigs.get( 1 ).setName( "Two updated" );
							return s.update( pigs.get( 0 ), pigs.get( 1 ) );
						} )
				) )
				.thenAccept( v -> assertSqlLogTracker( "update pig set name=.* where id=.*" ) )
				.thenCompose( v -> assertExpectedResult( PIGS_AFTER_UPDATE ) )
		);
	}

	private CompletionStage<Void> assertExpectedResult(Object[] expected) {
		return getSessionFactory().withStatelessTransaction( s -> s
				.createQuery( "from GuineaPig p order by id", Object.class )
				.getResultList()
				.thenAccept( pigs -> assertThat( pigs ).containsExactly( expected ) ) );
	}

	private static void assertSqlLogTracker(String queryRegex) {
		// We expect only one query for each batched operations
		assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
		// Parameters are different for different dbs, so the regex must keep that in consideration
		assertThat( sqlTracker.getLoggedQueries() ).allMatch( s -> s.matches( queryRegex ) );
	}

	@Entity(name = "GuineaPig")
	@Table(name = "pig")
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
