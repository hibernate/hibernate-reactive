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

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TimestampTest extends BaseReactiveTest {
	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Record.class );
		return configuration;
	}

	@Test
	public void test(TestContext context) {
		Record record = new Record();
		record.text = "initial text";
		test( context, getMutinySessionFactory()
				.withSession( session -> session.persist( record )
						.chain( session::flush )
						.invoke( () -> context.assertEquals(
								record.created.truncatedTo( ChronoUnit.HOURS ),
								record.updated.truncatedTo( ChronoUnit.HOURS )
						) )
						.invoke( () -> record.text = "edited text" )
						.chain( session::flush )
						.invoke( () -> assertInstants( context, record ) ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Record.class, record.id ) ) )
				.invoke( r -> assertInstants( context, record ) )
		);
	}

	private static void assertInstants(TestContext ctx, Record r) {
		ctx.assertNotNull( r.created );
		ctx.assertNotNull( r.updated );
		// Sometimes, when the test suite is fast enough, they might be the same
		ctx.assertTrue(
				r.updated.compareTo( r.created ) >= 0,
				"Updated instant is before created. Updated[" + r.updated + "], Created[" + r.created + "]"
		);
	}

	@Entity(name = "Record")
	static class Record {
		@GeneratedValue
		@Id
		long id;
		@Basic(optional = false)
		String text;
		@CreationTimestamp
		Instant created;
		@UpdateTimestamp
		Instant updated;
	}
}
