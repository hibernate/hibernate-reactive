/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.stat.EntityStatistics;
import org.hibernate.stat.QueryStatistics;
import org.hibernate.stat.Statistics;
import org.junit.After;
import org.junit.Test;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.hibernate.cfg.AvailableSettings.GENERATE_STATISTICS;


public class StatisticsTest extends BaseReactiveTest {

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass( Named.class );
        configuration.setProperty( GENERATE_STATISTICS, "true" );
        return configuration;
    }

    @After
    public void cleanDB(TestContext context) {
        getSessionFactory().close();
    }

    @Test
    public void testMutinyStatistics(TestContext context) {
        Statistics statistics = getMutinySessionFactory().getStatistics();
        test( context,
                getMutinySessionFactory()
                        .withTransaction(
                                (s, t) -> s.persistAll( new Named("foo"), new Named("bar"), new Named("baz") )
                        )
                        .chain( ()->
                                getMutinySessionFactory().withTransaction(
                                        (s, t) -> s.find(Named.class, 1).chain(s::remove)
                                ) )
                        .invoke( v-> {
                            context.assertEquals( 3L, statistics.getEntityInsertCount() );
                            context.assertEquals( 1L, statistics.getEntityLoadCount() );
                            context.assertEquals( 1L, statistics.getEntityDeleteCount() );

                            context.assertEquals( 2L, statistics.getFlushCount() );
                            context.assertEquals( 2L, statistics.getSessionOpenCount() );
                            context.assertEquals( 2L, statistics.getSessionCloseCount() );
//                            context.assertEquals( 2L, statistics.getTransactionCount() );
//                            context.assertEquals( 2L, statistics.getConnectCount() );
//                            context.assertEquals( 5L, statistics.getPrepareStatementCount() );

                            EntityStatistics entityStatistics = statistics.getEntityStatistics( Named.class.getName() );
                            context.assertEquals( 3L, entityStatistics.getInsertCount() );
                            context.assertEquals( 1L, entityStatistics.getLoadCount() );
                            context.assertEquals( 1L, entityStatistics.getDeleteCount() );

                            context.assertEquals( 0, statistics.getQueries().length );
                        } )
                        .chain( ()->
                                getMutinySessionFactory().withTransaction(
                                        (s, t) -> s.createQuery("from Named").getResultList()
                                ) )
                        .invoke( v-> {
                            context.assertEquals( 3L, statistics.getEntityInsertCount() );
                            context.assertEquals( 3L, statistics.getEntityLoadCount() );
                            context.assertEquals( 1L, statistics.getEntityDeleteCount() );
                            context.assertEquals( 1L, statistics.getQueryExecutionCount() );

                            context.assertEquals( 1, statistics.getQueries().length );

                            QueryStatistics queryStatistics = statistics.getQueryStatistics("from Named");
                            context.assertEquals( 1L, queryStatistics.getExecutionCount() );
                            context.assertEquals( 2L, queryStatistics.getExecutionRowCount() );
//                            context.assertNotEquals( 0L, queryStatistics.getExecutionMaxTime() );

                            context.assertEquals( 3L, statistics.getFlushCount() );
                            context.assertEquals( 3L, statistics.getSessionOpenCount() );
                            context.assertEquals( 3L, statistics.getSessionCloseCount() );
//                            context.assertEquals( 3L, statistics.getTransactionCount() );
//                            context.assertEquals( 3L, statistics.getConnectCount() );
//                            context.assertEquals( 6L, statistics.getPrepareStatementCount() );
                        } )
        );
    }

    @Test
    public void testStageStatistics(TestContext context) {
        Statistics statistics = getSessionFactory().getStatistics();
        test( context,
                getSessionFactory()
                        .withTransaction(
                                (s, t) -> s.persist( new Named("foo"), new Named("bar"), new Named("baz") )
                        )
                        .thenCompose( v ->
                                getSessionFactory().withTransaction(
                                        (s, t) -> s.find(Named.class, 1).thenCompose(s::remove)
                                ) )
                        .thenAccept( v-> {
                            context.assertEquals( 3L, statistics.getEntityInsertCount() );
                            context.assertEquals( 1L, statistics.getEntityLoadCount() );
                            context.assertEquals( 1L, statistics.getEntityDeleteCount() );

                            context.assertEquals( 2L, statistics.getFlushCount() );
                            context.assertEquals( 2L, statistics.getSessionOpenCount() );
                            context.assertEquals( 2L, statistics.getSessionCloseCount() );
//                            context.assertEquals( 2L, statistics.getTransactionCount() );
//                            context.assertEquals( 2L, statistics.getConnectCount() );
//                            context.assertEquals( 5L, statistics.getPrepareStatementCount() );

                            EntityStatistics entityStatistics = statistics.getEntityStatistics( Named.class.getName() );
                            context.assertEquals( 3L, entityStatistics.getInsertCount() );
                            context.assertEquals( 1L, entityStatistics.getLoadCount() );
                            context.assertEquals( 1L, entityStatistics.getDeleteCount() );

                            context.assertEquals( 0, statistics.getQueries().length );
                        } )
                        .thenCompose( v ->
                                getSessionFactory().withTransaction(
                                        (s, t) -> s.createQuery("from Named").getResultList()
                                ) )
                        .thenAccept( v-> {
                            context.assertEquals( 3L, statistics.getEntityInsertCount() );
                            context.assertEquals( 3L, statistics.getEntityLoadCount() );
                            context.assertEquals( 1L, statistics.getEntityDeleteCount() );
                            context.assertEquals( 1L, statistics.getQueryExecutionCount() );

                            context.assertEquals( 1, statistics.getQueries().length );

                            QueryStatistics queryStatistics = statistics.getQueryStatistics("from Named");
                            context.assertEquals( 1L, queryStatistics.getExecutionCount() );
                            context.assertEquals( 2L, queryStatistics.getExecutionRowCount() );
//                            context.assertNotEquals( 0L, queryStatistics.getExecutionMaxTime() );

                            context.assertEquals( 3L, statistics.getFlushCount() );
                            context.assertEquals( 3L, statistics.getSessionOpenCount() );
                            context.assertEquals( 3L, statistics.getSessionCloseCount() );
//                            context.assertEquals( 3L, statistics.getTransactionCount() );
//                            context.assertEquals( 3L, statistics.getConnectCount() );
//                            context.assertEquals( 6L, statistics.getPrepareStatementCount() );
                        } )
        );
    }

    @Entity(name="Named")
    @Table(name = "named_thing")
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
