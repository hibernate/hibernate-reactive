/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.junit.Rule;
import org.junit.Test;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDateTime;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

public class CustomSqlTest extends BaseReactiveTest {

    @Rule
    public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL, COCKROACHDB );

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass(Record.class);
        return configuration;
    }

    @Test
    public void test(TestContext context) {
        Record record = new Record();
        record.text = "initial text";
        test(context,
                getMutinySessionFactory()
                        .withSession( session -> session.persist(record)
                                .chain(session::flush)
                                .invoke( () -> record.text = "edited text" )
                                .chain(session::flush)
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withSession( session -> session.find(Record.class, record.id)
                                        .invoke( (result) -> {
                                            context.assertNotNull(result);
                                            context.assertEquals("edited text", result.text );
                                            context.assertNotNull(result.updated);
                                            context.assertNull(result.deleted);
                                        } )
                                        .chain(session::remove)
                                        .chain(session::flush)
                                ) )
                        .chain( () -> getMutinySessionFactory()
                                .withSession( session -> session.find(Record.class, record.id)
                                        .invoke( (result) -> {
                                            context.assertNotNull(result);
                                            context.assertEquals("edited text", result.text );
                                            context.assertNotNull(result.updated);
                                            context.assertNotNull(result.deleted);
                                        } )
                                ) )
        );
    }

    @Entity
    @Table(name="SqlRecord")
    @SQLInsert(sql = "insert into sqlrecord (text, updated, id) values ($1, localtimestamp, $2)")
    @SQLUpdate(sql = "update sqlrecord set text=$1, updated=localtimestamp where id=$2")
    @SQLDelete(sql = "update sqlrecord set deleted=localtimestamp where id=$1")
    static class Record {
        @GeneratedValue @Id long id;
        @Basic(optional = false) String text;
        @Column(insertable = false, updatable = false, nullable = false) LocalDateTime updated;
        @Column(insertable = false, updatable = false) LocalDateTime deleted;
    }
}
