/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.skipTestsFor;

/**
 * CompletionStage API doesn't guarantee that composing stages will be on the same thread.
 * <p>
 * This might result in an exception saying that no Vert.x context is active
 * when running the tests on slower machines like the images we use on CI.
 * <p>
 * The error doesn't always happen unless the tests run outside
 * an existing Vert.x context - that's why we don't extend {@link BaseReactiveTest}.
 */
public class NoVertxContextTest {

	@Rule
	public DatabaseSelectionRule dbRule = skipTestsFor( ORACLE );

	/**
	 * The number of entities to persist.
	 * Faster machines might need a higher number for this test to fail.
	 */
	private static final int ENTRIES_NUM = 1000;

	private static SessionFactory sessionFactory;

	@BeforeClass
	public static void setupSessionFactory() {
		final Configuration configuration = new BaseReactiveTest.ConfigurationBuilder()
				.build( List.of( GameCharacter.class ) );
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() );
		StandardServiceRegistry registry = builder.build();
		sessionFactory = configuration.buildSessionFactory( registry );
	}

	@AfterClass
	public static void closeSessionFactory() {
		sessionFactory.close();
	}

	@After
	public void deleteEntries() {
		mutinyFactory()
				.withTransaction( session -> session
				.createQuery( "delete from " + GameCharacter.ENTITY_NAME )
				.executeUpdate() )
				.await().indefinitely();
	}

	public Stage.SessionFactory stageFactory() {
		return sessionFactory.unwrap( Stage.SessionFactory.class );
	}

	@Test
	public void testWithStage() {
		String namePrefix = "Character N ";
		loopStage( 1, ENTRIES_NUM + 1, index -> persistWithStage( index, namePrefix + index ) );
		stageFactory().withSession( session -> session.find( GameCharacter.class, ENTRIES_NUM ) )
				.thenAccept( found -> {
					assertThat( found ).isNotNull();
					assertThat( found.id ).isEqualTo( ENTRIES_NUM );
					assertThat( found.name ).isEqualTo( namePrefix + ENTRIES_NUM );
				} )
				.thenCompose( v -> stageFactory().withSession( session -> session
						.createQuery( "select count(*) from " + GameCharacter.ENTITY_NAME )
						.getSingleResult() ) )
				.thenAccept( count -> assertThat( count ).isEqualTo( Long.valueOf( ENTRIES_NUM ) ) )
				.toCompletableFuture().join();
	}

	private CompletionStage<Void> persistWithStage(Integer id, String name) {
		return stageFactory().withTransaction( s -> s.persist( new GameCharacter( id, name ) ) );
	}

	private static void loopStage(int start, int end, Function<Integer, CompletionStage<Void>> consumer) {
		for ( int i = start; i < end; i++ ) {
			consumer.apply( i ).toCompletableFuture().join();
		}
	}

	public Mutiny.SessionFactory mutinyFactory() {
		return sessionFactory.unwrap( Mutiny.SessionFactory.class );
	}

	@Test
	public void testWithMutiny() {
		String namePrefix = "Character N ";
		loopUni( 1, ENTRIES_NUM + 1, index -> persistWithMutiny( index, namePrefix + index ) );
		mutinyFactory().withSession( session -> session.find( GameCharacter.class, ENTRIES_NUM ) )
				.invoke( found -> {
					assertThat( found ).isNotNull();
					assertThat( found.id ).isEqualTo( ENTRIES_NUM );
					assertThat( found.name ).isEqualTo( namePrefix + ENTRIES_NUM );
				} )
				.chain( () -> mutinyFactory().withSession( session -> session
						.createQuery( "select count(*) from " + GameCharacter.ENTITY_NAME ).getSingleResult() ) )
				.invoke( count -> assertThat( count ).isEqualTo( Long.valueOf( ENTRIES_NUM ) ) )
				.await().indefinitely();
	}

	private static void loopUni(int start, int end, Function<Integer, Uni<Void>> consumer) {
		for ( int i = start; i < end; i++ ) {
			consumer.apply( i ).await().indefinitely();
		}
	}

	private Uni<Void> persistWithMutiny(int id, String name) {
		return mutinyFactory().withTransaction( s -> s.persist( new GameCharacter( id, name ) ) );
	}

	@Entity(name = GameCharacter.ENTITY_NAME)
	@Table
	public static class GameCharacter {

		public static final String ENTITY_NAME = "GC";

		@Id
		public Integer id;

		@Column
		public String name;

		public GameCharacter() {

		}

		public GameCharacter(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

}
