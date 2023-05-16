/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test a find after a flush and before closing the session
 */
public class FindAfterFlushTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Webcomic.class );
	}

	@Test
	public void findAfterFlushWithStages(VertxTestContext context) {
		final Webcomic wc = new Webcomic( "Girls With Slingshots ", "Danielle Corsetto" );
		test( context, getSessionFactory().withSession( s -> s
				.persist( wc )
				.thenCompose( $ -> s.flush() )
				.thenCompose( $ -> s.find( Webcomic.class, wc.getId() ) )
				.thenAccept( found -> assertEquals( wc, found ) )
		) );
	}

	@Test
	public void findAfterFlushWithMutiny(VertxTestContext context) {
		final Webcomic wc = new Webcomic( "Saturday Morning Breakfast Cereal ", "Zach Weinersmith" );
		test( context, getMutinySessionFactory().withSession( s -> s
				.persist( wc )
				.call( s::flush )
				.chain( () -> s.find( Webcomic.class, wc.getId() ) )
				.invoke( found -> assertEquals( wc, found ) )
		) );
	}

	@Test
	public void withTransactionFindAfterFlushWithStages(VertxTestContext context) {
		final Webcomic wc = new Webcomic( "Something Positive", "R. K. Milholland" );
		test( context, getSessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( wc )
						.thenCompose( $ -> s.flush() )
						.thenCompose( $ -> s.find( Webcomic.class, wc.getId() ) )
						.thenAccept( found -> assertEquals( wc, found ) )
				)
		);
	}

	@Test
	public void withTransactionFindAfterFlushWithMutiny(VertxTestContext context) {
		final Webcomic wc = new Webcomic( "Questionable Content", "Jeph Jacques" );
		test( context, getMutinySessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( wc )
						.call( s::flush )
					  	.chain( () -> s.find( Webcomic.class, wc.getId() ) )
					  	.invoke( found -> {
							assertEquals( wc, found );
						} )
				)
		);
	}

	@Entity(name = "Webcomic")
	@Table(name = "WEBCOMIC")
	private static class Webcomic {
		@Id
		@GeneratedValue
		private Long id;
		private String creator;
		private String title;

		public Webcomic() {
		}

		public Webcomic(String creator, String title) {
			this.creator = creator;
			this.title = title;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getCreator() {
			return creator;
		}

		public void setCreator(String creator) {
			this.creator = creator;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Webcomic webComic = (Webcomic) o;
			return Objects.equals( creator, webComic.creator ) &&
					Objects.equals( title, webComic.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( creator, title );
		}

		@Override
		public String toString() {
			return title + " by " + creator;
		}
	}
}
