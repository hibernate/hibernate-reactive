/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;

import static org.junit.jupiter.api.Assertions.assertNull;

public class OneToOneLazyOrphanRemovalTest extends BaseReactiveTest {


	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Car.class, PaintColor.class, Engine.class );
	}

	private Uni<Void> populateDbMutiny() {
		final PaintColor color = new PaintColor( 1, "Red" );
		final Engine engine = new Engine( 1, 275 );
		final Car car = new Car( 1, engine, color );
		return getMutinySessionFactory().withTransaction( (s, t) -> s.persistAll( color, engine, car ) );
	}

	@Test
	public void testUnidirectionalOneToOneOrphanRemoval(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Car.class, 1 )
						.invoke( car -> car.setEngine( null ) ) ) )
				.call( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Car.class, 1 )
								.invoke( car -> assertNull( car.getEngine() ) )
								.chain( () -> session
										.find( Engine.class, 1 )
										.invoke( Assertions::assertNull ) ) ) )
		);
	}

	@Test
	public void testBidirectionalOneToOneOrphanRemoval(VertxTestContext context) {
		test( context, populateDbMutiny()
				.call( () -> getMutinySessionFactory()
				.withTransaction( (session, tx) -> session
						.find( Car.class, 1 )
						.invoke( car -> {
							car.getPaintColor().setCar( null );
							car.setPaintColor( null );
						} ) ) )
				.call( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Car.class, 1 )
								.invoke( car -> assertNull( car.getPaintColor() ) )
								.chain( () -> session
										.find( PaintColor.class, 1 )
										.invoke( Assertions::assertNull ) ) ) )
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
