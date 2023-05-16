/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;


import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.Cache;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Cacheable;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.hibernate.annotations.CacheConcurrencyStrategy.NONSTRICT_READ_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CacheTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Named.class );
		configuration.setProperty( Environment.USE_SECOND_LEVEL_CACHE, "true" );
		configuration.setProperty( Environment.CACHE_REGION_FACTORY, "org.hibernate.cache.jcache.JCacheRegionFactory" );
		configuration.setProperty( "hibernate.javax.cache.provider", "org.ehcache.jsr107.EhcacheCachingProvider" );
		configuration.setProperty( "hibernate.javax.cache.uri", "/ehcache.xml" );
		return configuration;
	}

	@Override
	public CompletionStage<Void> cleanDb() {
		getSessionFactory().close();
		return CompletionStages.voidFuture();
	}

	@Test
	public void testCacheWithHQL(VertxTestContext context) {
		org.hibernate.Cache cache = getSessionFactory().getCache();
		test(
				context,
				getSessionFactory().withTransaction(
								(s, t) -> s.persist( new Named( "foo" ), new Named( "bar" ), new Named( "baz" ) )
						)
						.thenAccept( v -> {
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
						//populate the cache
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createQuery( "from Named" ).getResultList()
										.thenAccept( list -> assertEquals( 3, list.size() ) )
						) )
						.thenAccept( v -> {
							assertTrue( cache.contains( Named.class, 1 ) );
							assertTrue( cache.contains( Named.class, 2 ) );
							assertTrue( cache.contains( Named.class, 3 ) );
						} )
						//read stuff from the cache
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 1 )
										.thenAccept( n -> assertEquals( "foo", n.name ) )
						) )
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 2 )
										.thenAccept( n -> assertEquals( "bar", n.name ) )
						) )
						//change the database
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createQuery( "update Named set name='x'||name" ).executeUpdate()
						) )
						.thenAccept( v -> {
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
						//read stuff from the database
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 1 )
										.thenAccept( n -> assertEquals( "xfoo", n.name ) )
						) )
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 2 )
										.thenAccept( n -> assertEquals( "xbar", n.name ) )
						) )
						.thenAccept( v -> {
							assertTrue( cache.contains( Named.class, 1 ) );
							assertTrue( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
							//evict the region
							cache.evict( Named.class );
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
						//repopulate the cache
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createQuery( "from Named" ).getResultList()
										.thenAccept( list -> assertEquals( 3, list.size() ) )
						) )
						.thenAccept( v -> {
							assertTrue( cache.contains( Named.class, 1 ) );
							assertTrue( cache.contains( Named.class, 2 ) );
							assertTrue( cache.contains( Named.class, 3 ) );
						} )
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 1 )
										.thenAccept( n -> assertEquals( "xfoo", n.name ) )
						) )
						//change the database
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createQuery( "delete Named" ).executeUpdate()
						) )
						.thenAccept( v -> {
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
		);
	}

	@Test
	public void testCacheWithNativeSQL(VertxTestContext context) {
		org.hibernate.Cache cache = getSessionFactory().getCache();
		test(
				context,
				getSessionFactory().withTransaction(
								(s, t) -> s.persist( new Named( "foo" ), new Named( "bar" ), new Named( "baz" ) )
						)
						.thenAccept( v -> {
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
						//populate the cache
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createNativeQuery( "select * from named_thing", Named.class ).getResultList()
										.thenAccept( list -> assertEquals( 3, list.size() ) )
						) )
						.thenAccept( v -> {
							assertTrue( cache.contains( Named.class, 1 ) );
							assertTrue( cache.contains( Named.class, 2 ) );
							assertTrue( cache.contains( Named.class, 3 ) );
						} )
						//read stuff from the cache
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 1 )
										.thenAccept( n -> assertEquals( "foo", n.name ) )
						) )
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 2 )
										.thenAccept( n -> assertEquals( "bar", n.name ) )
						) )
						//change the database
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createNativeQuery( "update named_thing set name=concat('x',name)" )
										.executeUpdate()
						) )
						.thenAccept( v -> {
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
						//read stuff from the database
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 1 )
										.thenAccept( n -> assertEquals( "xfoo", n.name ) )
						) )
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 2 )
										.thenAccept( n -> assertEquals( "xbar", n.name ) )
						) )
						.thenAccept( v -> {
							assertTrue( cache.contains( Named.class, 1 ) );
							assertTrue( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
							//evict the region
							cache.evict( Named.class );
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
						//repopulate the cache
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createNativeQuery( "select * from named_thing", Named.class ).getResultList()
										.thenAccept( list -> assertEquals( 3, list.size() ) )
						) )
						.thenAccept( v -> {
							assertTrue( cache.contains( Named.class, 1 ) );
							assertTrue( cache.contains( Named.class, 2 ) );
							assertTrue( cache.contains( Named.class, 3 ) );
						} )
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.find( Named.class, 1 )
										.thenAccept( n -> assertEquals( "xfoo", n.name ) )
						) )
						//change the database
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createNativeQuery( "delete from named_thing" ).executeUpdate()
						) )
						.thenAccept( v -> {
							assertFalse( cache.contains( Named.class, 1 ) );
							assertFalse( cache.contains( Named.class, 2 ) );
							assertFalse( cache.contains( Named.class, 3 ) );
						} )
		);
	}

	@Entity(name = "Named")
	@Table(name = "named_thing")
	@Cacheable
	@Cache(region = "named", usage = NONSTRICT_READ_WRITE)
	static class Named {
		@Id
		@GeneratedValue
		Integer id;
		String name;

		public Named(String name) {
			this.name = name;
		}

		public Named() {
		}
	}

}
