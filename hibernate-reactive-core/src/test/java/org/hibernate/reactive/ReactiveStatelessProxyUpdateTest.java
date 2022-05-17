/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.HibernateException;
import org.hibernate.LazyInitializationException;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

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

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Game.class, GameCharacter.class );
	}

	@Test
	public void testUnfetchedEntityException(TestContext context) {
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
				.invoke( exception -> assertThat( exception.getMessage() ).contains( "HR000072" ) )
		);
	}

	@Test
	public void testLazyInitializationException(TestContext context) {
		Game lol = new Game( "League of Legends" );
		GameCharacter ck = new GameCharacter( "Caitlyn Kiramman" );
		ck.setGame( lol );

		test( context, assertThrown( LazyInitializationException.class, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( lol, ck ) )
				.chain( targetId -> getMutinySessionFactory()
						.withTransaction( session -> session.find( GameCharacter.class, ck.getId() ) )
				)
				.call( charFound -> getMutinySessionFactory()
						.withStatelessTransaction( s -> {
							Game game = charFound.getGame();
							// LazyInitializationException here because we haven't fetched the entity
							game.setGameTitle( "League of Legends V2" );
							context.fail( "We were expecting a LazyInitializationException" );
							return null;
						} )
				)
		) );
	}

	@Test
	public void testUpdateWithInitializedProxy(TestContext context) {
		Game lol = new Game( "League of Legends" );
		GameCharacter ck = new GameCharacter( "Caitlyn Kiramman" );
		ck.setGame( lol );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( lol, ck ) )
				.chain( targetId -> getMutinySessionFactory()
						.withTransaction( session -> session.find( GameCharacter.class, ck.getId() ) )
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
						.createQuery( "from Game", Game.class )
						.getSingleResult()
						.invoke( result -> context.assertEquals( "League of Legends V2", result.getGameTitle() ) ) )
				)
		);
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

		@ManyToOne
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
