/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class OneToOneLazyOrphanRemovalTest extends BaseReactiveTest {
	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Car.class );
		configuration.addAnnotatedClass( PaintColor.class );
		configuration.addAnnotatedClass( Engine.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		final PaintColor color = new PaintColor( 1, "Red" );
		final Engine engine = new Engine( 1, 275 );
		final Car car = new Car( 1, engine, color );
		test( context, getSessionFactory()
				.withTransaction( (session, tx) -> session.persist( color, engine, car ) ) );
	}

	@After
	public void deleteAll(TestContext context) {
		test( context, deleteEntities( "Car", "PaintColor", "Engine" ) );
	}

	@Test
	public void testUnidirectionalOneToOneOrphanRemoval(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Car.class, 1 )
						.invoke( car -> car.setEngine( null ) ) )
				.call( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Car.class, 1 )
								.invoke( car -> context
										.assertNull( car.getEngine() ) )
								.chain( () -> session
										.find( Engine.class, 1 )
										.invoke( context::assertNull ) ) ) )
		);
	}

	@Test
	public void testBidirectionalOneToOneOrphanRemoval(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Car.class, 1 )
						.invoke( car -> {
							car.getPaintColor().setCar( null );
							car.setPaintColor( null );
						} ) )
				.call( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Car.class, 1 )
								.invoke( car -> context
										.assertNull( car.getPaintColor() ) )
								.chain( () -> session
										.find( PaintColor.class, 1 )
										.invoke( context::assertNull ) ) ) )
		);
	}

	@Entity(name = "Car")
	public static class Car {
		@Id
		private Integer id;

		// represents a bidirectional one-to-one
		@OneToOne(orphanRemoval = true)
		private PaintColor paintColor;

		// represents a unidirectional one-to-one
		@OneToOne(orphanRemoval = true)
		private Engine engine;

		Car() {
			// Required by JPA
		}

		Car(Integer id, Engine engine, PaintColor paintColor) {
			this.id = id;
			this.engine = engine;
			this.paintColor = paintColor;
			paintColor.setCar( this );
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public PaintColor getPaintColor() {
			return paintColor;
		}

		public void setPaintColor(PaintColor paintColor) {
			this.paintColor = paintColor;
		}

		public Engine getEngine() {
			return engine;
		}

		public void setEngine(Engine engine) {
			this.engine = engine;
		}
	}

	@Entity(name = "Engine")
	public static class Engine {
		@Id
		private Integer id;
		private Integer horsePower;

		Engine() {
			// Required by JPA
		}

		Engine(Integer id, int horsePower) {
			this.id = id;
			this.horsePower = horsePower;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public Integer getHorsePower() {
			return horsePower;
		}

		public void setHorsePower(Integer horsePower) {
			this.horsePower = horsePower;
		}
	}

	@Entity(name = "PaintColor")
	public static class PaintColor {
		@Id
		private Integer id;
		private String color;

		@OneToOne(mappedBy = "paintColor")
		private Car car;

		PaintColor() {
			// Required by JPA
		}

		PaintColor(Integer id, String color) {
			this.id = id;
			this.color = color;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getColor() {
			return color;
		}

		public void setColor(String color) {
			this.color = color;
		}

		public Car getCar() {
			return car;
		}

		public void setCar(Car car) {
			this.car = car;
		}
	}
}
