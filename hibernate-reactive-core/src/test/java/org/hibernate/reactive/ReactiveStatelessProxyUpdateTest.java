/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.function.IntFunction;

import org.hibernate.HibernateException;
import org.hibernate.LazyInitializationException;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the stateless update of a proxy.
 * <p>
 * Note that it's required to update and read the values from the proxy using getter/setter and
 * there is no guarantee about this working otherwise.
 * </p>
 *
 * @see org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl#reactiveUpdate(Object)
 * @see ReactiveStatelessSessionTest
 */
public class ReactiveStatelessProxyUpdateTest extends BaseReactiveTest {

	/**
	 * Number of updates. On slower machines like CI this test will fail even with a slow number of entries.
	 * On faster machines, it needs a lot more updates.
	 */
	private final static int UPDATES_NUM = 10;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Game.class, GameCharacter.class );
	}

	@Test
	public void testUnfetchedEntityException(VertxTestContext context) {
		Game lol = new Game( "League of Legends" );
		GameCharacter ck = new GameCharacter( "Caitlyn Kiramman" );
		ck.setGame( lol );

		test( context, assertThrown( HibernateException.class, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( lol, ck ) )
				.chain( targetId -> getMutinySessionFactory().withTransaction( session -> session
						.find( GameCharacter.class, ck.getId() ) )
				)
				.call( charFound -> getMutinySessionFactory()
						.withStatelessTransaction( s -> {
							Game game = charFound.getGame();
							game.gameTitle = "League of Legends V2";
							// We expect the update to fail because we haven't fetched the entity
							// and therefore the proxy is uninitialized
							return s.update( game );
						} )
				) )
				.invoke( exception -> assertTrue( exception.getMessage().contains( "HR000072" ) ) )
		);
	}

	@Test
	public void testLazyInitializationException(VertxTestContext context) {
		Game lol = new Game( "League of Legends" );
		GameCharacter ck = new GameCharacter( "Caitlyn Kiramman" );
		ck.setGame( lol );

		test( context, assertThrown( LazyInitializationException.class, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( lol, ck ) )
				.chain( targetId -> getMutinySessionFactory()
						.withStatelessSession( session -> session.get( GameCharacter.class, ck.getId() ) )
				)
				.call( charFound -> getMutinySessionFactory()
						.withStatelessTransaction( s -> {
							Game game = charFound.getGame();
							// LazyInitializationException here because we haven't fetched the entity
							game.setGameTitle( "League of Legends V2" );
							context.failNow( "We were expecting a LazyInitializationException" );
							return null;
						} )
				)
		) );
	}

	@Test
	public void testUpdateWithInitializedProxyInLoop(VertxTestContext context) {
		test( context, loop( 0, UPDATES_NUM, i -> {
			Game lol = new Game( "League of Legends" );
			GameCharacter ck = new GameCharacter( "Caitlyn Kiramman" );
			ck.setGame( lol );

			return getMutinySessionFactory()
					.withTransaction( s -> s.persistAll( lol, ck ) )
					.chain( targetId -> getMutinySessionFactory()
							.withSession( session -> session.find( GameCharacter.class, ck.getId() ) )
					)
					.chain( charFound -> getMutinySessionFactory()
							.withStatelessTransaction( s -> s
									// The update of the associated entity should work if we fetch it first
									.fetch( charFound.getGame() )
									.chain( fetchedGame -> {
										fetchedGame.setGameTitle( "League of Legends V2" );
										return s.update( fetchedGame );
									} )
							)
					)
					.call( () -> getMutinySessionFactory().withSession( session -> session
							.find( Game.class, lol.getId() )
							.invoke( result -> assertEquals( "League of Legends V2", result.getGameTitle() ) ) )
					);
		} ) );
	}

	private Uni<Void> loop(int start, int end, IntFunction<Uni<Void>> intFun) {
		return Uni.createFrom()
				.completionStage( CompletionStages.loop( start, end, index -> intFun.apply( index ).subscribeAsCompletionStage() ) );
	}

	@Entity(name = "Game")
	@Table(name = "game")
	public static class Game {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		@Column(name = "title")
		private String gameTitle;

		public Game() {
		}

		public Game(String gameTitle) {
			this.gameTitle = gameTitle;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getGameTitle() {
			return gameTitle;
		}

		public void setGameTitle(String gameTitle) {
			this.gameTitle = gameTitle;
		}

		@Override
		public String toString() {
			return id + ":" + gameTitle;
		}
	}

	@Entity(name = "GameCharacter")
	@Table(name = "game_character")
	public static class GameCharacter {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		private String name;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "game_id", referencedColumnName = "id")
		private Game game;

		public GameCharacter() {
		}

		public GameCharacter(String name) {
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Game getGame() {
			return game;
		}

		public void setGame(Game game) {
			this.game = game;
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + game;
		}
	}
}
