/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.Cache;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.junit.Test;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import static org.hibernate.annotations.CacheConcurrencyStrategy.NONSTRICT_READ_WRITE;

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

    @Test
    public void testCache(TestContext context) {
        test( context,
                getSessionFactory().withTransaction(
                        (s, t) -> s.persist( new Named("foo"), new Named("bar"), new Named("baz") )
                )
                        //populate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("from Named" ).getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        //change the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("update Named set name='x'||name" ).executeUpdate()
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("foo", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 2)
                                        .thenAccept( n-> context.assertEquals("bar", n.name) )
                        ) )
                        //repopulate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("from Named" )/*.setCacheMode(CacheMode.REFRESH)*/.getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
        );
    }

    @Test
    public void testCache2(TestContext context) {
        test( context,
                getSessionFactory().withTransaction(
                        (s, t) -> s.persist( new Named("foo"), new Named("bar"), new Named("baz") )
                )
                        //populate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("from Named" ).getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        //change the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("update Named set name='x'||name" ).executeUpdate()
                        ) )
                        .thenAccept( v-> getSessionFactory().getCache().evict(Named.class) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 2)
                                        .thenAccept( n-> context.assertEquals("xbar", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 2)
                                        .thenAccept( n-> context.assertEquals("xbar", n.name) )
                        ) )
                        //repopulate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("from Named" )/*.setCacheMode(CacheMode.REFRESH)*/.getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
        );
    }

    @Entity(name="Named")
    @Cacheable
    @Cache(region="named", usage=NONSTRICT_READ_WRITE)
    static class Named {
        @Id @GeneratedValue
        Integer id;
        String name;

        public Named(String name) {
            this.name = name;
        }

        public Named() {}
    }

}
