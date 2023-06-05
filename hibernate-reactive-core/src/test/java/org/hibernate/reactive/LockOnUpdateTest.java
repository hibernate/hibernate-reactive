/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.LockMode;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)
public class LockOnUpdateTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
	}

	@Test
	public void testLockDuringUpdate(VertxTestContext context) {
		Record secondRecord = new Record( "FIRST" );
		secondRecord.text = "I'm the first record";
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( session -> session.persist( secondRecord )
								.call( session::flush )
								.chain( () -> this.updateRecord( "FIRST", true ) )
						)
						.chain( () -> getMutinySessionFactory().withSession( session -> session
										.find( Record.class, secondRecord.name ) ) )
						.invoke( record -> assertTrue( record.text.contains( "updated" ) ) )
		);
	}

	@Test
	public void testLockBeforeUpdate(VertxTestContext context) {
		Record secondRecord = new Record( "SECOND" );
		secondRecord.text = "I'm the second record";
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( session -> session.persist( secondRecord )
								.call( session::flush )
								.call( () -> session.lock( secondRecord, LockMode.PESSIMISTIC_WRITE ) )
								.chain( () -> this.updateRecord( "SECOND", false ) )
						)
						.chain( () -> getMutinySessionFactory().withSession( session -> session
								.find( Record.class, secondRecord.name ) ) )
						.invoke( (record) -> assertTrue( record.text.contains( "updated" ) ) )
		);
	}

	private Uni<Record> updateRecord(final String name, final boolean doLock) {
		if ( doLock ) {
			return getMutinySessionFactory()
					.withTransaction( session -> session
							.find( Record.class, name, LockMode.PESSIMISTIC_WRITE
					) )
					.map( entity -> entity.setText( "I'm an updated record" ) );
		}

		return getMutinySessionFactory()
				.withTransaction( session -> session.find( Record.class, name ) )
				.map( entity -> entity.setText( "I'm an updated record" ) );

	}

	@Entity(name = "Record")
	@Table(name = "FRecord")
	static class Record {
		@Id
		String name;
		@Basic(optional = false)
		String text;

		public Record() {
		}

		public Record(final String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public Record setName(final String name) {
			this.name = name;
			return this;
		}

		public Record setText(final String text) {
			this.text = text;
			return this;
		}
	}
}
