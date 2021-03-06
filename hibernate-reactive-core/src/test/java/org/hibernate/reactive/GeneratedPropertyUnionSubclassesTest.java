/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.CurrentUser.LoggedUserGenerator;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.runOnlyFor;

/**
 * Test the @{@link Generated} annotation with {@link InheritanceType#TABLE_PER_CLASS}
 * <p>
 *     Note that id generation using Identity is not a valid mapping.
 * </p>
 */
public class GeneratedPropertyUnionSubclassesTest extends BaseReactiveTest {

	// It requires native queries so we only test this on Postgres
	@Rule
	public DatabaseSelectionRule selectionRule = runOnlyFor( POSTGRESQL, COCKROACHDB );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GeneratedRegular.class );
		return configuration;
	}

	@Test
	public void testRegularEntity(TestContext context) {
		final GeneratedRegular davide = new GeneratedRegular( "Davide", "D'Alto" );

		CurrentUser.INSTANCE.logIn( "dd-insert" );
		test( context,
			  getMutinySessionFactory()
					  // Generated during insert
					  .withSession( session -> session.persist( davide ).call( session::flush )
							  .invoke( v -> {
								  context.assertNotNull( davide.id );
								  context.assertEquals( "Davide", davide.firstname );
								  context.assertEquals( "D'Alto", davide.lastname );
								  context.assertEquals( "Davide D'Alto", davide.fullName );
								  context.assertNotNull( davide.createdAt );
								  context.assertEquals( "dd-insert", davide.createdBy );
								  context.assertEquals( "dd-insert", davide.updatedBy );
								  context.assertNull( davide.never );
								  CurrentUser.INSTANCE.logOut();
							  } ) )
					  // Generated during update
					  .chain( () -> getMutinySessionFactory()
							  .withSession( session -> session.find( GeneratedRegular.class, davide.id )
									  .chain( result -> {
										  CurrentUser.INSTANCE.logIn( "dd-update" );
										  result.lastname = "O'Tall";
										  return session.flush().invoke( afterUpdate -> {
											  context.assertEquals( "Davide", result.firstname );
											  context.assertEquals( "O'Tall", result.lastname );
											  context.assertEquals( "Davide O'Tall", result.fullName );
											  context.assertEquals( davide.createdAt, result.createdAt );
											  context.assertEquals( "dd-insert", result.createdBy );
											  context.assertEquals( "dd-update", result.updatedBy );
											  context.assertNull( result.never );
											  CurrentUser.INSTANCE.logOut();
										  } );
									  } ) ) )
		);
	}

	@Entity
	@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
	static class GeneratedRegularParent {
		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		public Long id;

		public String firstname;

		public String lastname;

		@Generated(GenerationTime.ALWAYS)
		@Column(columnDefinition = "varchar(600) GENERATED ALWAYS AS (firstname || ' ' || lastname) STORED")
		public String fullName;

		@GeneratorType(type = LoggedUserGenerator.class, when = GenerationTime.INSERT)
		public String createdBy;

		public GeneratedRegularParent() {
		}

		public GeneratedRegularParent(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	@Entity
	static class GeneratedRegular extends GeneratedRegularParent {
		@Temporal(value = TemporalType.TIMESTAMP)
		@Generated(GenerationTime.INSERT)
		@ColumnDefault("CURRENT_TIMESTAMP")
		public Date createdAt;

		@GeneratorType(type = LoggedUserGenerator.class, when = GenerationTime.ALWAYS)
		public String updatedBy;

		@Generated(GenerationTime.NEVER)
		public String never;

		public GeneratedRegular() {
		}

		public GeneratedRegular(String firstname, String lastname) {
			super( firstname, lastname );
		}
	}

}
