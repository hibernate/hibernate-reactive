/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import org.junit.Test;

import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

public class TimestampTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
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
