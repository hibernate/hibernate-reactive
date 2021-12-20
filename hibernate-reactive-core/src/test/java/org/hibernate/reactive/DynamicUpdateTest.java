/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.OptimisticLocking;
import org.hibernate.cfg.Configuration;

import org.junit.Test;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import static org.hibernate.annotations.OptimisticLockType.DIRTY;

public class DynamicUpdateTest extends BaseReactiveTest {

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
		test(
				context,
				getMutinySessionFactory()
						.withSession( session -> session.persist( record )
								.chain( session::flush )
								.invoke( () -> record.text = "edited text" )
								.chain( session::flush )
						)
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( Record.class, record.id )
										.invoke( (result) -> {
											context.assertNotNull( result );
											context.assertEquals( "edited text", result.text );
										} )
										.chain( session::remove )
										.chain( session::flush )
								) )
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( Record.class, record.id )
										.invoke( context::assertNull )
								) )
		);
	}

	@Entity
	@Table(name = "BigRecord")
	@DynamicInsert
	@DynamicUpdate
	@OptimisticLocking(type = DIRTY)
	static class Record {
		@GeneratedValue
		@Id
		long id;
		@Basic(optional = false)
		String text;
		String moreText;
		Double number;
		String evenMoreText;
	}
}
