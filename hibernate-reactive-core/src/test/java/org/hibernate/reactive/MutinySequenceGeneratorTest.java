package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Version;
import java.util.Objects;

public class MutinySequenceGeneratorTest extends BaseMutinyTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( SequenceId.class );
		return configuration;
	}

	@Test
	public void testSequenceGenerator(TestContext context) {

		SequenceId b = new SequenceId();
		b.string = "Hello World";

		test( context,
				openSession()
				.flatMap(s -> s.persist(b))
				.flatMap(s -> s.flush())
				.flatMap( v -> openSession())
				.flatMap( s2 ->
					s2.find( SequenceId.class, b.getId() )
						.onItem().invoke( bb -> {
							context.assertNotNull( bb );
							context.assertEquals( bb.id, 5 );
							context.assertEquals( bb.string, b.string );
							context.assertEquals( bb.version, 0 );

							bb.string = "Goodbye";
						})
						.flatMap(vv -> s2.flush())
						.flatMap(vv -> s2.find( SequenceId.class, b.getId() ))
						.onItem().invoke( bt -> {
							context.assertEquals( bt.version, 1 );
						}))
				.flatMap( v -> openSession())
				.flatMap( s3 -> s3.find( SequenceId.class, b.getId() ) )
				.onItem().invoke( bb -> {
					context.assertEquals(bb.version, 1);
					context.assertEquals(bb.string, "Goodbye");
				})
				.convert().toCompletionStage()
		);
	}

	@Entity
	@SequenceGenerator(name = "seq",
			sequenceName = "test_id_seq",
			initialValue = 5)
	public static class SequenceId {
		@Id @GeneratedValue(generator = "seq")
		Integer id;
		@Version Integer version;
		String string;

		public SequenceId() {
		}

		public SequenceId(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			SequenceId sequenceId = (SequenceId) o;
			return Objects.equals(string, sequenceId.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
