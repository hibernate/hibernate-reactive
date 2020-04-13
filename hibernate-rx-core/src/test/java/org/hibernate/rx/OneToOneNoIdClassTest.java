package org.hibernate.rx;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import java.io.Serializable;
import java.util.Objects;

public class OneToOneNoIdClassTest extends BaseRxTest {
	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( AnEntity.class );
		configuration.addAnnotatedClass( OtherEntity.class );
		return configuration;
	}

	@Test
	public void testLoad(TestContext context) {
		final OtherEntity otherEntity = new OtherEntity( 1, "Other Entity" );
		final AnEntity anEntity = new AnEntity( otherEntity, "An Entity" );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( otherEntity ) )
						.thenCompose( s -> s.persist( anEntity ) )
						.thenCompose( s -> s.flush() )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( AnEntity.class, 1 )
								.thenAccept( optionalAnEntity -> {
									context.assertTrue( optionalAnEntity.isPresent() );
									context.assertEquals( anEntity, optionalAnEntity.get() );
									context.assertEquals( otherEntity, optionalAnEntity.get().otherEntity );
								})
						)
		);
	}

	@Entity(name = "AnEntity")
	public static class AnEntity implements Serializable {
		@Id
		@OneToOne
		private OtherEntity otherEntity;
		private String name;

		AnEntity() {
		}

		AnEntity(OtherEntity otherEntity, String name) {
			this.otherEntity = otherEntity;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AnEntity anEntity = (AnEntity) o;
			return otherEntity.equals(anEntity.otherEntity) &&
					Objects.equals(name, anEntity.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(otherEntity, name);
		}
	}

	@Entity(name = "OtherEntity")
	public static class OtherEntity implements Serializable {

		@Id
		private int id;
		private String name;

		OtherEntity() {
		}

		OtherEntity(int id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			OtherEntity that = (OtherEntity) o;
			return id == that.id &&
					Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name);
		}
	}
}

