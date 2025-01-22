/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;


public class IdentityGenerationWithBatchingTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.STATEMENT_BATCH_SIZE, "5" );
		return configuration;
	}

	@Test
	public void test(VertxTestContext context) {
		String[] titles = {
				"Around the World in Eighty Days",
				"The Book of M: A Novel",
				"The Poppy War",
				"The poetry home repair manual",
				"Relish"
		};
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( asBooks( titles ) ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> s.createSelectionQuery( "from Book order by id ASC", Book.class ).getResultList() ) )
				.invoke( results -> {
					String[] resultTitles = results.stream().map( Book::getTitle ).toArray( String[]::new );
					assertThat( resultTitles ).containsExactly( titles );
					Long savedId = 0L;
					for ( Book book : results ) {
						assertThat( book.getId() ).isNotNull().isGreaterThan( savedId );
						savedId = book.getId();
					}
				} )
		);
	}

	private Book[] asBooks(String[] titles) {
		return Arrays.stream( titles ).map( Book::new ).toArray( Book[]::new );
	}

	@Entity(name = "Book")
	@Table(name = "books")
	static class Book {

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		private String title;

		public Book() {
		}

		public Book(String title) {
			this.title = title;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		@Override
		public String toString() {
			return id + ":" + title;
		}
	}
}
