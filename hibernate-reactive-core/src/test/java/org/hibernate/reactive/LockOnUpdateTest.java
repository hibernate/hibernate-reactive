/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.LockMode;

import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

/**
 * @author Barry LaFond
 */
public class LockOnUpdateTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
	}

	@Test
	public void testLockDuringUpdate(TestContext context) {
		Record secondRecord = new Record( "FIRST" );
		secondRecord.text = "I'm the first record";
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( session -> session.persist( secondRecord )
								.call( session::flush )
								.chain( () -> this.updateRecord( "FIRST", true ) )
						)
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( Record.class,
										secondRecord.name
								) ) )
						.invoke( (record) -> context.assertTrue( record.text.contains( "updated" ) ) )
		);
	}

	@Test
	public void testLockBeforeUpdate(TestContext context) {
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
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( Record.class,
										secondRecord.name
								) ) )
						.invoke( (record) -> context.assertTrue( record.text.contains( "updated" ) ) )
		);
	}

	private Uni<Record> updateRecord(final String name, final boolean doLock) {
		if ( doLock ) {
			return getMutinySessionFactory()
					.withTransaction( session -> session.find( Record.class, name,
							LockMode.PESSIMISTIC_WRITE
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
