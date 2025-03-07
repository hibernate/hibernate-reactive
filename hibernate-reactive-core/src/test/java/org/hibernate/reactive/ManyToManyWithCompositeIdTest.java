/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.junit5.Timeout;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

@Timeout(value = 10, timeUnit = MINUTES)
public class ManyToManyWithCompositeIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( CarsClients.class, ClientA.class, Client.class, Car.class );
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		return voidFuture();
	}

	@Test
	public void test(VertxTestContext context) {
		List<Client> clients = new ArrayList<>();
		for ( int i = 0; i < 5; i++ ) {
			ClientA client = new ClientA();
			client.setName( "name" + i );
			client.setEmail( "email" + i );
			client.setPhone( "phone" + i );
			clients.add( client );
		}

		List<Car> cars = new ArrayList<>();
		for ( int i = 0; i < 2; i++ ) {
			Car car = new Car();
			car.setBrand( "brand" + i );
			car.setModel( "model" + i );
			cars.add( car );
		}

		test( context, getMutinySessionFactory()
				.withSession( session -> {
					session.setBatchSize( 5 );
					return session.persistAll( cars.toArray() )
							.chain( () -> session
									.persistAll( clients.toArray() )
									.chain( session::flush ) )
							.chain( () -> {
								List<CarsClients> carsClientsList = new ArrayList<>();
								for ( Client client : clients ) {
									for ( Car car : cars ) {
										CarsClients carsClients = new CarsClients( "location" );
										carsClientsList.add( carsClients );
										car.addClient( carsClients );
										client.addCar( carsClients );
									}
								}
								return session
										.persistAll( carsClientsList.toArray() )
										.chain( session::flush );
							} );
				} )
		);
	}

	@Entity(name = "Car")
	@Table(name = "Car")
	public static class Car {

		@Id
		@SequenceGenerator(name = "seq_car", sequenceName = "id_seq_car", allocationSize = 1)
		@GeneratedValue(generator = "seq_car", strategy = GenerationType.SEQUENCE)
		private Long id;

		public String brand;


		private String model;

		@OneToMany(mappedBy = "car")
		private Set<CarsClients> clients = new HashSet<>();

		public Car() {
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getBrand() {
			return brand;
		}

		public void setBrand(String brand) {
			this.brand = brand;
		}

		public String getModel() {
			return model;
		}

		public void setModel(String model) {
			this.model = model;
		}

		public Set<CarsClients> getClients() {
			return clients;
		}

		public void setClients(Set<CarsClients> clients) {
			this.clients = clients;
		}

		public void addClient(CarsClients carsClients) {
			carsClients.setCar( this );
			clients.add( carsClients );
		}
	}

	@Entity
	@Table(name = "`Client`")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static class Client {

		@Id
		@SequenceGenerator(name = "seq", sequenceName = "id_seq", allocationSize = 1)
		@GeneratedValue(generator = "seq", strategy = GenerationType.SEQUENCE)
		private Long id;

		private String name;

		private String email;

		private String phone;

		@OneToMany(mappedBy = "client")
		private Set<CarsClients> cars = new HashSet<>();

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String getPhone() {
			return phone;
		}

		public void setPhone(String phone) {
			this.phone = phone;
		}

		public Set<CarsClients> getCars() {
			return cars;
		}

		public void setCars(Set<CarsClients> cars) {
			this.cars = cars;
		}

		public void addCar(CarsClients carsClients) {
			carsClients.setClient( this );
			cars.add( carsClients );
		}
	}

	@Entity
	@Table(name = "`ClientA`")
	public static class ClientA extends Client {

		public ClientA() {
		}
	}

	@Entity
	@IdClass(CarsClientsId.class)
	@Table(name = "cars_clients")
	public static class CarsClients {

		@Id
		@ManyToOne
		private Car car;

		@Id
		@ManyToOne
		private Client client;

		private String location;

		public CarsClients() {
		}

		public CarsClients(String location) {
			this.location = location;
		}

		public Car getCar() {
			return car;
		}

		public void setCar(Car car) {
			this.car = car;
		}

		public Client getClient() {
			return client;
		}

		public void setClient(Client client) {
			this.client = client;
		}

		public String getLocation() {
			return location;
		}

		public void setLocation(String location) {
			this.location = location;
		}
	}

	public static class CarsClientsId {
		private Car car;

		private Client client;

		public CarsClientsId() {
		}

		public Car getCar() {
			return car;
		}

		public void setCar(Car car) {
			this.car = car;
		}

		public Client getClient() {
			return client;
		}

		public void setClient(Client client) {
			this.client = client;
		}
	}
}
