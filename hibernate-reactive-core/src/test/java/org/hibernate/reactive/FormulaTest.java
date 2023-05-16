/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.Formula;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;


public class FormulaTest extends BaseReactiveTest {

	@RegisterExtension
	public DBSelectionExtension dbSelection = DBSelectionExtension.skipTestsFor( MARIA, MYSQL );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
	}

	@Test
	public void test(VertxTestContext context) {
		Record record = new Record();
		record.text = "initial text";
		test( context, getMutinySessionFactory()
				.withSession( session -> session
						.persist( record )
						.chain( session::flush ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session.find( Record.class, record.id ) ) )
				.map( Record::getCurrent )
				.invoke( Assertions::assertNotNull )
		);
	}

	@Entity(name = "Record")
	@Table(name = "FRecord")
	static class Record {
		@GeneratedValue
		@Id
		long id;
		@Basic(optional = false)
		String text;
		@Formula("current_timestamp")
		LocalDateTime current;

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public String getText() {
			return text;
		}

		public void setText(String text) {
			this.text = text;
		}

		public LocalDateTime getCurrent() {
			return current;
		}

		public void setCurrent(LocalDateTime current) {
			this.current = current;
		}
	}
}
