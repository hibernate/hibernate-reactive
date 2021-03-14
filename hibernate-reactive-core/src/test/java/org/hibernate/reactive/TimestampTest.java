/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.*;
import java.time.Instant;
import java.time.OffsetDateTime;

public class TimestampTest extends BaseReactiveTest {
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
        record.expiry=OffsetDateTime.now();
        test(context,
                getMutinySessionFactory()
                        .withSession( session -> session.persist(record)
                                .chain(session::flush)
                                .invoke( () -> {
                                    context.assertNotNull(record.created);
                                    context.assertNotNull(record.updated);
                                } )
                                .invoke( () -> {
                                    record.text = "edited text";
                                    record.expiry=OffsetDateTime.now().plusHours(1);
                                } )
                                .chain(session::flush)
                                .invoke( () -> {
                                    context.assertTrue( record.expiry.isBefore(OffsetDateTime.now().plusHours(2)));
                                    context.assertNotNull(record.created);
                                    context.assertNotNull(record.updated);
                                    context.assertTrue( record.updated.isAfter(record.created) );
                                } )
                        ).chain( () -> getMutinySessionFactory()
                        .withSession( session -> session.find(Record.class, record.id) ) )
                        .invoke( (r) -> {
                            context.assertNotNull(r.created);
                            context.assertNotNull(r.expiry);
                            context.assertNotNull(r.updated);
                            context.assertTrue( r.updated.isAfter(r.created) );
                        } )
        );
    }
    @Entity(name="Record")
    static class Record {
        @GeneratedValue @Id long id;
        @Basic(optional = false) String text;
        @CreationTimestamp Instant created;
        @UpdateTimestamp Instant updated;
        @Column OffsetDateTime expiry;
    }
}
