package org.hibernate.rx;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.cfg.Configuration;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class FetchSelectEagerUniManyToOneTest extends BaseRxTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Department.class );
		configuration.addAnnotatedClass( Employee.class );
		return configuration;
	}

	@Test
	public void testPersistAndFind(TestContext context) {
		final Department  panopticon = new Department( "PNP", "Panoptcon" );
		final Employee frederick = new Employee( "FRD", "Frederick Langston" );
		frederick.setDepartment( panopticon );

		final Department janitorialDep = new Department( "JNT", "Janitorial Department" );
		final Employee ahti  = new Employee( "AHT", "Ahti" );
		ahti.setDepartment( janitorialDep );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist(  janitorialDep ) )
						.thenCompose( s -> s.persist(  panopticon ) )
						.thenCompose( s -> s.persist(  frederick ) )
						.thenCompose( s -> s.persist(  ahti ) )
						.thenCompose( s -> s.flush() )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find(  Employee.class, ahti.getId() ) )
						.thenAccept( optionalEmployee -> {
							context.assertTrue( optionalEmployee.isPresent() );
							context.assertEquals( optionalEmployee.get(), ahti );
							context.assertEquals( optionalEmployee.get().getDepartment(), janitorialDep );
						} )
		);
	}

	/**
	 * Federal Bureau Of Control Department.
	 */
	@Entity(name = "Department")
	public static class Department {

		@Id
		private String id;

		private String name;

		public Department() {
		}

		public Department(String id, String name) {
			this.id = id;
			this.name = name;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Department that = (Department) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return name;
		}
	}

	@Entity(name = "Employee")
	public static class Employee {

		@Id
		private String id;

		private String fullname;

		@ManyToOne(fetch = FetchType.EAGER)
		@Fetch(FetchMode.SELECT)
		private Department department;

		public Employee() {
		}

		public Employee(String id, String fullname) {
			this.id = id;
			this.fullname = fullname;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFullname() {
			return fullname;
		}

		public void setFullname(String fullname) {
			this.fullname = fullname;
		}

		public Department getDepartment() {
			return department;
		}

		public void setDepartment(Department department) {
			this.department = department;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Employee employee = (Employee) o;
			return Objects.equals( fullname, employee.fullname );
		}

		@Override
		public int hashCode() {
			return Objects.hash( fullname );
		}

		@Override
		public String toString() {
			return fullname;
		}
	}
}
