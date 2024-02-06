/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.awt.Point;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.Struct;
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = ORACLE, reason = "see issue https://github.com/hibernate/hibernate-reactive/issues/1855")
@DisabledFor(value = {SQLSERVER, MYSQL,	MARIA, COCKROACHDB}, reason = "ORM does not support @Struct for these databases")
public class StructEmbeddableTest extends BaseReactiveTest {
	static RecordStructHolder holder1;
	static RecordStructHolder holder2;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( RecordStructHolder.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		holder1 = new RecordStructHolder( 1L, new NamedPoint( "first", 1, 1 ) );
		holder2 = new RecordStructHolder( 2L, new NamedPoint( "second", 2, 2 ) );
		holder1.simpleStringHolder = new SimpleStringHolder( "column a","column b","column c" );

		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( holder1, holder2 )
						.thenCompose( v -> session.flush() ) )
		);
	}

	@Test
	public void testFindAndUpdate(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s2 -> s2.find( RecordStructHolder.class, holder1.id )
						.thenAccept( resultHolder -> {
							assertNotNull( resultHolder );
							assertEquals( holder1.getThePoint().getPoint(), resultHolder.getThePoint().getPoint() );
							resultHolder.setThePoint( new NamedPoint( "third", 3, 3 ) );
							assertEquals( "third", resultHolder.getThePoint().name );
						} )
						.thenCompose( vv -> s2.flush() )
						.thenCompose( vv -> s2.find( RecordStructHolder.class, holder1.id )
								.thenAccept( found -> assertEquals( "third", found.getThePoint().getName() ) ) )
				)
		);
	}

	@Test
	public void testSelectionItems(VertxTestContext context) {
		test( context, openSession()
					  .thenCompose( s -> s.createSelectionQuery( "from RecordStructHolder where id = ?1", RecordStructHolder.class )
						.setParameter( 1, holder1.getId() )
						.getResultList() )
						.thenAccept( holders -> {
							assertNotNull( holders );
							final RecordStructHolder holder = holders.get( 0 );
							assertEquals( holder1.getThePoint().getPoint(), holder.getThePoint().getPoint() );
						} )
		);
	}

	@Test
	public void testEmbeddedColumnOrder(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s2 -> s2.find( RecordStructHolder.class, holder1.id )
						.thenAccept( resultHolder -> {
							assertNotNull( resultHolder );
							assertEquals( holder1.getThePoint().getPoint(), resultHolder.getThePoint().getPoint() );
							assertEquals( "column a", holder1.simpleStringHolder.aColumn );
							assertEquals( "column b", holder1.simpleStringHolder.bColumn );
							assertEquals( "column c", holder1.simpleStringHolder.cColumn );
						} )
				)
		);
	}

	@Entity(name = "RecordStructHolder")
	public static class RecordStructHolder {
		@Id
		private Long id;
		@Struct(name = "my_point_type")
		private NamedPoint thePoint;

		private SimpleStringHolder simpleStringHolder;

		public RecordStructHolder() {
		}

		public RecordStructHolder(Long id, NamedPoint thePoint) {
			this.id = id;
			this.thePoint = thePoint;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public NamedPoint getThePoint() {
			return thePoint;
		}

		public void setThePoint(NamedPoint point) {
			this.thePoint = point;
		}
	}

	@Embeddable
	static class NamedPoint {
		public String name;
		public Point point;

		public NamedPoint() {
		}

		public NamedPoint(String name, Integer x, Integer y) {
			this.point = new Point( x, y );
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public Point getPoint() {
			return point;
		}
	}

	// By default, the order of columns is based on the alphabetical ordering of the embeddable type attribute names.
	// This class has column names re-defined using @Column annotation "name" attribute and will reverse the column order
	@Embeddable
	@Struct(name = "simple_string_holder")
	static class SimpleStringHolder {
		@Column(name = "c")
		public String aColumn;
		@Column(name = "b")
		public String bColumn;
		@Column(name = "a")
		public String cColumn;

		public SimpleStringHolder() {}

		public SimpleStringHolder(String aColumn, String bColumn, String cColumn) {
			this.aColumn = aColumn;
			this.bColumn = bColumn;
			this.cColumn = cColumn;
		}
	}
}
