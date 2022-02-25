/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.Cache;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import org.junit.After;
import org.junit.Test;

import jakarta.persistence.Cacheable;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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

    @After
    public void cleanDB(TestContext context) {
        getSessionFactory().close();
    }

    @Test
    public void testCacheWithHQL(TestContext context) {
        org.hibernate.Cache cache = getSessionFactory().getCache();
        test( context,
                getSessionFactory().withTransaction(
                        (s, t) -> s.persist( new Named("foo"), new Named("bar"), new Named("baz") )
                )
                        .thenAccept( v-> {
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
                        //populate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("from Named" ).getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        .thenAccept( v-> {
                            context.assertTrue( cache.contains(Named.class, 1) );
                            context.assertTrue( cache.contains(Named.class, 2) );
                            context.assertTrue( cache.contains(Named.class, 3) );
                        } )
                        //read stuff from the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("foo", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 2)
                                        .thenAccept( n-> context.assertEquals("bar", n.name) )
                        ) )
                        //change the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("update Named set name='x'||name" ).executeUpdate()
                        ) )
                        .thenAccept( v-> {
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
                        //read stuff from the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 2)
                                        .thenAccept( n-> context.assertEquals("xbar", n.name) )
                        ) )
                        .thenAccept( v-> {
                            context.assertTrue( cache.contains(Named.class, 1) );
                            context.assertTrue( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                            //evict the region
                            cache.evict(Named.class);
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
                        //repopulate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("from Named" ).getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        .thenAccept( v-> {
                            context.assertTrue( cache.contains(Named.class, 1) );
                            context.assertTrue( cache.contains(Named.class, 2) );
                            context.assertTrue( cache.contains(Named.class, 3) );
                        } )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
                        //change the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createQuery("delete Named" ).executeUpdate()
                        ) )
                        .thenAccept( v-> {
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
        );
    }

    @Test
    public void testCacheWithNativeSQL(TestContext context) {
        org.hibernate.Cache cache = getSessionFactory().getCache();
        test( context,
                getSessionFactory().withTransaction(
                        (s, t) -> s.persist( new Named("foo"), new Named("bar"), new Named("baz") )
                )
                        .thenAccept( v-> {
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
                        //populate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createNativeQuery("select * from named_thing", Named.class ).getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        .thenAccept( v-> {
                            context.assertTrue( cache.contains(Named.class, 1) );
                            context.assertTrue( cache.contains(Named.class, 2) );
                            context.assertTrue( cache.contains(Named.class, 3) );
                        } )
                        //read stuff from the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("foo", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 2)
                                        .thenAccept( n-> context.assertEquals("bar", n.name) )
                        ) )
                        //change the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createNativeQuery("update named_thing set name=concat('x',name)" ).executeUpdate()
                        ) )
                        .thenAccept( v-> {
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
                        //read stuff from the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 2)
                                        .thenAccept( n-> context.assertEquals("xbar", n.name) )
                        ) )
                        .thenAccept( v-> {
                            context.assertTrue( cache.contains(Named.class, 1) );
                            context.assertTrue( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                            //evict the region
                            cache.evict(Named.class);
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
                        //repopulate the cache
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createNativeQuery("select * from named_thing", Named.class ).getResultList()
                                        .thenAccept( list -> context.assertEquals(3, list.size()) )
                        ) )
                        .thenAccept( v-> {
                            context.assertTrue( cache.contains(Named.class, 1) );
                            context.assertTrue( cache.contains(Named.class, 2) );
                            context.assertTrue( cache.contains(Named.class, 3) );
                        } )
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.find(Named.class, 1)
                                        .thenAccept( n-> context.assertEquals("xfoo", n.name) )
                        ) )
                        //change the database
                        .thenCompose( v-> getSessionFactory().withSession(
                                s -> s.createNativeQuery("delete from named_thing" ).executeUpdate()
                        ) )
                        .thenAccept( v-> {
                            context.assertFalse( cache.contains(Named.class, 1) );
                            context.assertFalse( cache.contains(Named.class, 2) );
                            context.assertFalse( cache.contains(Named.class, 3) );
                        } )
        );
    }

    @Entity(name="Named")
    @Table(name = "named_thing")
    @Cacheable
    @Cache(region="reg.named", usage=NONSTRICT_READ_WRITE)
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
