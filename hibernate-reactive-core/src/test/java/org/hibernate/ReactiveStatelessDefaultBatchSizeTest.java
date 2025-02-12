/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The test aims to check that methods accepting the batch size as parameter e.g. {@link Stage.StatelessSession#insert(int, Object...)}
 * work when {@link AvailableSettings.STATEMENT_BATCH_SIZE} hasn't been set.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class ReactiveStatelessDefaultBatchSizeTest extends BaseReactiveTest {
	private static SqlStatementTracker sqlTracker;

	private static final String PIG_ONE_NAME = "One";
	private static final String PIG_TWO_NAME = "Two";
	private static final String PIG_THREE_NAME = "Three";
	private static final String PIG_FOUR_NAME = "Four";
	private static final String PIG_FIVE_NAME = "Five";
	private static final String PIG_SIX_NAME = "Six";

	private static final GuineaPig PIG_ONE = new GuineaPig( 11, PIG_ONE_NAME );
	private static final GuineaPig PIG_TWO = new GuineaPig( 22, PIG_TWO_NAME );
	private static final GuineaPig PIG_THREE = new GuineaPig( 33, PIG_THREE_NAME );
	private static final GuineaPig PIG_FOUR = new GuineaPig( 44, PIG_FOUR_NAME );
	private static final GuineaPig PIG_FIVE = new GuineaPig( 55, PIG_FIVE_NAME );
	private static final GuineaPig PIG_SIX = new GuineaPig( 66, PIG_SIX_NAME );

	private static final GuineaPig[] PIGS = { PIG_ONE, PIG_TWO, PIG_THREE, PIG_FOUR, PIG_FIVE, PIG_SIX, };

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
				ReactiveStatelessDefaultBatchSizeTest::filter,
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
		String[] accepted = { "insert ", "update ", "delete " };
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
	}

	@Test
	public void testMutinyBatchingInsert(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( () -> {
					// We expect only one insert query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches( "insert into pig \\(name,id\\) values (.*)" );
				} )
		);
	}

	@Test
	public void testMutinyBatchingInsertMultiple(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s.insertMultiple( List.of( PIGS ) ) )
				.invoke( () -> {
					// We expect only one insert query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches( "insert into pig \\(name,id\\) values (.*)" );
				} )
				.invoke( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
								 .createQuery( "select p from GuineaPig p", GuineaPig.class )
								 .getResultList()
								 .invoke( pigs -> assertThat( pigs ).hasSize( PIGS.length ) )
						 )
				)
		);
	}

	@Test
	public void testMutinyBatchingInsertAllNoBatchSizeParameter(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s.insertAll( PIGS ) )
				.invoke( () -> {
					// We expect only one insert query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches( "insert into pig \\(name,id\\) values (.*)" );
				} )
				.invoke( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
								 .createQuery( "select p from GuineaPig p", GuineaPig.class )
								 .getResultList()
								 .invoke( pigs -> assertThat( pigs ).hasSize( PIGS.length ) )
						 )
				)
		);
	}

	@Test
	public void testStageBatchingInsert(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> {
					// We expect only one insert query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches( "insert into pig \\(name,id\\) values (.*)" );
				} )
				.thenAccept( v -> getSessionFactory().withStatelessTransaction( s -> s
									 .createQuery( "select p from GuineaPig p", GuineaPig.class )
									 .getResultList()
									 .thenAccept( pigs -> assertThat( pigs ).hasSize( PIGS.length ) )
							 )
				)
		);
	}

	@Test
	public void testStageBatchingInsertMultiple(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( s -> s.insertMultiple( List.of(PIGS) ) )
				.thenAccept( v -> {
					// We expect only one insert query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches( "insert into pig \\(name,id\\) values (.*)" );
				} )
				.thenAccept( v -> getSessionFactory().withStatelessTransaction( s -> s
									 .createQuery( "select p from GuineaPig p", GuineaPig.class )
									 .getResultList()
									 .thenAccept( pigs -> assertThat( pigs ).hasSize( PIGS.length ) )
							 )
				)
		);
	}

	@Test
	public void testStageBatchingInsertNoBatchSizeParameter(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( s -> s.insert( PIGS ) )
				.thenAccept( v -> {
					// We expect only one insert query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches( "insert into pig \\(name,id\\) values (.*)" );
				} )
				.thenAccept( v -> getSessionFactory().withStatelessTransaction( s -> s
									 .createQuery( "select p from GuineaPig p", GuineaPig.class )
									 .getResultList()
									 .thenAccept( pigs -> assertThat( pigs ).hasSize( PIGS.length ) )
							 )
				)
		);
	}

	@Test
	public void testMutinyBatchingDelete(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction(s -> s
								.createQuery( "select p from GuineaPig p", GuineaPig.class ).getResultList()
						)
						.invoke( pigs -> sqlTracker.clear() )
						.chain( pigs -> getMutinySessionFactory().withStatelessTransaction(
								s ->
										s.deleteAll( 10, pigs.subList( 0, 2 ).toArray() )
								 )
						)
						.invoke( () -> {
							// We expect only one delete query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "delete from pig where id=.*" );
						} )
						.chain( () -> getMutinySessionFactory().withStatelessTransaction( s -> s
												.createQuery( "select p from GuineaPig p", GuineaPig.class )
												.getResultList()
										)
								.invoke( guineaPigs -> assertThat( guineaPigs.size() ).isEqualTo( 4 ) )
						) )
		);
	}

	@Test
	public void testMutinyBatchingDeleteMultiple(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s.insertAll( 10, PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction(s -> s
								.createQuery( "select p from GuineaPig p", GuineaPig.class ).getResultList()
						)
						.invoke( pigs -> sqlTracker.clear() )
						.chain( pigs -> getMutinySessionFactory().withStatelessTransaction(
								s -> s.deleteMultiple( pigs.subList( 0, 2 ) ) )
						)
						.invoke( () -> {
							// We expect only one delete query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "delete from pig where id=.*" );
						} )
						.chain( () -> getMutinySessionFactory().withStatelessTransaction( s -> s
												.createQuery( "select p from GuineaPig p", GuineaPig.class )
												.getResultList()
										)
								.invoke( guineaPigs -> assertThat( guineaPigs.size() ).isEqualTo( 4 ) )
						) )
		);
	}

	@Test
	public void testMutinyBatchingDeleteAllNoBatchSizeParameter(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s.insertAll( PIGS ) )
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction(s -> s
								.createQuery( "select p from GuineaPig p", GuineaPig.class ).getResultList()
						)
						.invoke( pigs -> sqlTracker.clear() )
						.chain( pigs -> getMutinySessionFactory().withStatelessTransaction(
								s -> s.deleteAll( pigs.subList( 0, 2 ).toArray() ) )
						)
						.invoke( () -> {
							// We expect only one delete query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "delete from pig where id=.*" );
						} )
						.chain( () -> getMutinySessionFactory().withStatelessTransaction( s -> s
												.createQuery( "select p from GuineaPig p", GuineaPig.class )
												.getResultList()
										)
								.invoke( guineaPigs -> assertThat( guineaPigs.size() ).isEqualTo( 4 ) )
						) )
		);
	}

	@Test
	public void testStageBatchingDelete(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> sqlTracker.clear() )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
								.createQuery( "select p from GuineaPig p", GuineaPig.class ).getResultList()
								.thenCompose( pigs -> {
												  sqlTracker.clear();
												  return s.delete( 10, pigs.subList( 0, 2 ).toArray() );
											  }
								) )
						.thenAccept( vo -> {
							// We expect only one delete query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
									.matches( "delete from pig where id=.*" );
						} )
						.thenCompose( vo -> getSessionFactory().withStatelessTransaction( s -> s
													  .createQuery( "select p from GuineaPig p", GuineaPig.class )
													  .getResultList()
											  )
											  .thenAccept( guineaPigs -> assertThat( guineaPigs.size() ).isEqualTo( 4 ) )
						) )
		);
	}

	@Test
	public void testStageBatchingDeleteMultiple(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> sqlTracker.clear() )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
								.createQuery( "select p from GuineaPig p", GuineaPig.class ).getResultList()
								.thenCompose( pigs -> {
												  sqlTracker.clear();
												  return s.deleteMultiple( pigs.subList( 0, 2 ) );
											  }
								) )
						.thenAccept( vo -> {
							// We expect only one delete query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
									.matches( "delete from pig where id=.*" );
						} )
						.thenCompose( vo -> getSessionFactory().withStatelessTransaction( s -> s
										.createQuery( "select p from GuineaPig p", GuineaPig.class )
										.getResultList() )
								.thenAccept( guineaPigs -> assertThat( guineaPigs.size() ).isEqualTo( 4 ) )
						) )
		);
	}

	@Test
	public void testStageBatchingDeleteNoBatchSizeParameter(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> sqlTracker.clear() )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
								.createQuery( "select p from GuineaPig p", GuineaPig.class ).getResultList()
								.thenCompose( pigs -> {
												  sqlTracker.clear();
												  return s.delete( pigs.subList( 0, 2 ).toArray() );
											  }
								) )
						.thenAccept( vo -> {
							// We expect only one delete query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
									.matches( "delete from pig where id=.*" );
						} )
						.thenCompose( vo -> getSessionFactory().withStatelessTransaction( s -> s
										.createQuery( "select p from GuineaPig p", GuineaPig.class )
										.getResultList() )
								.thenAccept( guineaPigs -> assertThat( guineaPigs.size() ).isEqualTo( 4 ) )
						) )
		);
	}

	@Test
	public void testMutinyBatchingUpdate(VertxTestContext context) {
		final String pigOneUpdatedName = "One updated";
		final String pigTwoUpdatedName = "Two updated";
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s .insertAll( 10, PIGS ))
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "select p from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.invoke( pigs -> sqlTracker.clear() )
						.chain( pigs -> {
							GuineaPig guineaPigOne = pigs.get( 0 );
							guineaPigOne.setName( pigOneUpdatedName );
							GuineaPig guineaPigTwo = pigs.get( 1 );
							guineaPigTwo.setName( pigTwoUpdatedName );
							return s.updateAll( 10, new GuineaPig[] { guineaPigOne, guineaPigTwo } );
						} )
				) )
				.invoke( () -> {
					// We expect only one update query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "update pig set name=.* where id=.*" );
				} )
				.chain( () -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "select p from GuineaPig p order by id", GuineaPig.class )
						.getResultList()
						.invoke( guineaPigs -> {
							checkPigsAreCorrectlyUpdated( guineaPigs, pigOneUpdatedName, pigTwoUpdatedName );
						} ) ) )
		);
	}

	@Test
	public void testMutinyBatchingUpdateMultiple(VertxTestContext context) {
		final String pigOneUpdatedName = "One updated";
		final String pigTwoUpdatedName = "Two updated";
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s .insertAll( 10, PIGS ))
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "select p from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.invoke( pigs -> sqlTracker.clear() )
						.chain( pigs -> {
							GuineaPig guineaPigOne = pigs.get( 0 );
							guineaPigOne.setName( pigOneUpdatedName );
							GuineaPig guineaPigTwo = pigs.get( 1 );
							guineaPigTwo.setName( pigTwoUpdatedName );
							return s.updateMultiple( List.of( guineaPigOne, guineaPigTwo ) );
						} )
				) )
				.invoke( () -> {
					// We expect only one update query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "update pig set name=.* where id=.*" );
				} )
				.chain( () -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "select p from GuineaPig p order by id", GuineaPig.class )
						.getResultList()
						.invoke( guineaPigs -> {
							checkPigsAreCorrectlyUpdated( guineaPigs, pigOneUpdatedName, pigTwoUpdatedName );
						} ) ) )
		);
	}

	@Test
	public void testMutinyBatchingUpdateAllNoBatchSizeParameter(VertxTestContext context) {
		final String pigOneUpdatedName = "One updated";
		final String pigTwoUpdatedName = "Two updated";
		test( context, getMutinySessionFactory().withStatelessTransaction( s -> s .insertAll( 10, PIGS ))
				.invoke( sqlTracker::clear )
				.chain( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "select p from GuineaPig p order by p.id", GuineaPig.class )
						.getResultList()
						.invoke( pigs -> sqlTracker.clear() )
						.chain( pigs -> {
							GuineaPig guineaPigOne = pigs.get( 0 );
							guineaPigOne.setName( pigOneUpdatedName );
							GuineaPig guineaPigTwo = pigs.get( 1 );
							guineaPigTwo.setName( pigTwoUpdatedName );
							return s.updateAll( guineaPigOne, guineaPigTwo );
						} )
				) )
				.invoke( () -> {
					// We expect only one update query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "update pig set name=.* where id=.*" );
				} )
				.chain( () -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.createQuery( "select p from GuineaPig p order by id", GuineaPig.class )
						.getResultList()
						.invoke( guineaPigs -> {
							checkPigsAreCorrectlyUpdated( guineaPigs, pigOneUpdatedName, pigTwoUpdatedName );
						} ) ) )
		);
	}

	@Test
	public void testStageBatchingUpdate(VertxTestContext context) {
		final String pigOneUpdatedName = "One updated";
		final String pigTwoUpdatedName = "Two updated";
		test(context, getSessionFactory().withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> sqlTracker.clear() )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction(s -> s
								.createQuery( "select p from GuineaPig p order by p.id", GuineaPig.class )
								.getResultList()
								.thenApply( pigs -> {
									sqlTracker.clear();
									GuineaPig guineaPigOne = pigs.get( 0 );
									guineaPigOne.setName( pigOneUpdatedName );
									GuineaPig guineaPigTwo = pigs.get( 1 );
									guineaPigTwo.setName( pigTwoUpdatedName );
									return s.update( 10, new GuineaPig[] { guineaPigOne, guineaPigTwo } );
								} )
						)
						.thenAccept( vo -> {
							// We expect only one update query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "update pig set name=.* where id=.*" );
						} )
						.thenCompose( vo -> getSessionFactory().withStatelessTransaction( s -> s
								.createQuery( "select p from GuineaPig p order by id", GuineaPig.class )
								.getResultList()
								.thenAccept( guineaPigs ->
									checkPigsAreCorrectlyUpdated( guineaPigs, pigOneUpdatedName, pigTwoUpdatedName )
								 )
						) ) )
		);
	}

	@Test
	public void testStageBatchingUpdateMultiple(VertxTestContext context) {
		final String pigOneUpdatedName = "One updated";
		final String pigTwoUpdatedName = "Two updated";
		test(context, getSessionFactory().withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> sqlTracker.clear() )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction(s -> s
								.createQuery( "select p from GuineaPig p order by p.id", GuineaPig.class )
								.getResultList()
								.thenApply( pigs -> {
									sqlTracker.clear();
									GuineaPig guineaPigOne = pigs.get( 0 );
									guineaPigOne.setName( pigOneUpdatedName );
									GuineaPig guineaPigTwo = pigs.get( 1 );
									guineaPigTwo.setName( pigTwoUpdatedName );
									return s.updateMultiple( List.of( guineaPigOne, guineaPigTwo ) );
								} )
						)
						.thenAccept( vo -> {
							// We expect only one update query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "update pig set name=.* where id=.*" );
						} )
						.thenCompose( vo -> getSessionFactory().withStatelessTransaction( s -> s
								.createQuery( "select p from GuineaPig p order by id", GuineaPig.class )
								.getResultList()
								.thenAccept( guineaPigs ->
									checkPigsAreCorrectlyUpdated( guineaPigs, pigOneUpdatedName, pigTwoUpdatedName )
								 )
						) ) )
		);
	}

	@Test
	public void testStageBatchingUpdateNoBatchSizeParameter(VertxTestContext context) {
		final String pigOneUpdatedName = "One updated";
		final String pigTwoUpdatedName = "Two updated";
		test(context, getSessionFactory().withStatelessTransaction( s -> s.insert( 10, PIGS ) )
				.thenAccept( v -> sqlTracker.clear() )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction(s -> s
								.createQuery( "select p from GuineaPig p order by p.id", GuineaPig.class )
								.getResultList()
								.thenApply( pigs -> {
									sqlTracker.clear();
									GuineaPig guineaPigOne = pigs.get( 0 );
									guineaPigOne.setName( pigOneUpdatedName );
									GuineaPig guineaPigTwo = pigs.get( 1 );
									guineaPigTwo.setName( pigTwoUpdatedName );
									return s.update( guineaPigOne, guineaPigTwo );
								} )
						)
						.thenAccept( vo -> {
							// We expect only one update query
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).matches( "update pig set name=.* where id=.*" );
						} )
						.thenCompose( vo -> getSessionFactory().withStatelessTransaction( s -> s
								.createQuery( "select p from GuineaPig p order by id", GuineaPig.class )
								.getResultList()
								.thenAccept( guineaPigs ->
									checkPigsAreCorrectlyUpdated( guineaPigs, pigOneUpdatedName, pigTwoUpdatedName )
								 )
						) ) )
		);
	}

	private static void checkPigsAreCorrectlyUpdated(List<GuineaPig> guineaPigs, String pigOneUpdatedName, String pigTwoUpdatedName) {
		assertThat( guineaPigs.get( 0 ).getName() ).isEqualTo( pigOneUpdatedName );
		assertThat( guineaPigs.get( 1 ).getName() ).isEqualTo( pigTwoUpdatedName );
		assertThat( guineaPigs.get( 2 ).getName() ).isEqualTo( PIG_THREE_NAME );
		assertThat( guineaPigs.get( 3 ).getName() ).isEqualTo( PIG_FOUR_NAME );
		assertThat( guineaPigs.get( 4 ).getName() ).isEqualTo( PIG_FIVE_NAME );
		assertThat( guineaPigs.get( 5 ).getName() ).isEqualTo( PIG_SIX_NAME );
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
