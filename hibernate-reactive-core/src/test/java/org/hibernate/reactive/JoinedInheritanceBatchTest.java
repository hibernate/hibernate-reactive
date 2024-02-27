/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;


import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

@Timeout(value = 10, timeUnit = MINUTES)
public class JoinedInheritanceBatchTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ClientA.class, Client.class );
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		return voidFuture();
	}

	@Test
	public void test(VertxTestContext context) {
		final ClientA client1 = new ClientA("Client 1", "email@c1", "123456");

		test( context, getMutinySessionFactory().withTransaction( session -> {
						  session.setBatchSize( 5 );
						  return session.persist( client1 );
					  } )
					  .chain( () -> getMutinySessionFactory().withTransaction( session -> session
							  .createQuery( "select c from Client c", Client.class )
							  .getResultList()
							  .invoke( persistedClients -> assertThat( persistedClients )
									  .as( "Clients has not bee persisted" )
									  .isNotEmpty() ) ) )
		);
	}

	@Entity(name = "Client")
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

		public Client() {
		}

		public Client(String name, String email, String phone) {
			this.name = name;
			this.email = email;
			this.phone = phone;
		}

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

	}

	@Entity
	@Table(name = "`ClientA`")
	public static class ClientA extends Client {

		public ClientA() {
		}

		public ClientA(String name, String email, String phone) {
			super( name, email, phone );
		}
	}

}
