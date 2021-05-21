/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

/**
 * Test a find after a flush and before closing the session
 */
public class FindAfterFlushTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Webcomic.class );
		return configuration;
	}

	@After
	public void cleanDB(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (session, tx) -> session
						.createQuery( "delete from Webcomic" )
						.executeUpdate() ) );
	}

	@Test
	public void findAfterFlushWithStages(TestContext context) {
		final Webcomic wc = new Webcomic( "Girls With Slingshots ", "Danielle Corsetto" );
		test( context, getSessionFactory().withSession( s -> s
				.persist( wc )
				.thenCompose( $ -> s.flush() )
				.thenCompose( $ -> s.find( Webcomic.class, wc.getId() ) )
				.thenAccept( found -> context.assertEquals( wc, found ) )
		) );
	}

	@Test
	public void findAfterFlushWithMutiny(TestContext context) {
		final Webcomic wc = new Webcomic( "Saturday Morning Breakfast Cereal ", "Zach Weinersmith" );
		test( context, getMutinySessionFactory().withSession( s -> s
				.persist( wc )
				.call( s::flush )
				.chain( () -> s.find( Webcomic.class, wc.getId() ) )
				.invoke( found -> context.assertEquals( wc, found ) )
		) );
	}

	@Test
	public void withTransactionFindAfterFlushWithStages(TestContext context) {
		final Webcomic wc = new Webcomic( "Something Positive", "R. K. Milholland" );
		test( context, getSessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( wc )
						.thenCompose( $ -> s.flush() )
						.thenCompose( $ -> s.find( Webcomic.class, wc.getId() ) )
						.thenAccept( found -> context.assertEquals( wc, found ) )
				)
		);
	}

	@Test
	public void withTransactionFindAfterFlushWithMutiny(TestContext context) {
		final Webcomic wc = new Webcomic( "Questionable Content", "Jeph Jacques" );
		test( context, getMutinySessionFactory()
				.withTransaction( (s, tx) -> s
						.persist( wc )
						.call( s::flush )
					  	.chain( () -> s.find( Webcomic.class, wc.getId() ) )
					  	.invoke( found -> {
							context.assertEquals( wc, found );
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
