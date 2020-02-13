package org.hibernate.rx;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.id.enhanced.StandardOptimizerDescriptor;
import org.junit.Test;

import javax.persistence.*;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Tests a sequence using the "hilo" optimizer.
 */
public class SequenceHiLoGeneratorTest extends BaseRxTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty(
				AvailableSettings.PREFERRED_POOLED_OPTIMIZER,
				StandardOptimizerDescriptor.HILO.getExternalName()
		);
		configuration.setProperty(
				AvailableSettings.GENERATE_STATISTICS,
				"true"
		);
		configuration.addAnnotatedClass(SequenceId.class);
		return configuration;
	}

	@Test
	public void testSequenceGenerator(TestContext context) {

		CompletionStage<RxSession> sessionStage = openSession();

		for (int i = 0; i < 7; i++) {
			SequenceId sequenceId = new SequenceId();
			// set sequenceId.string so the integer suffix is the same as its id
			sequenceId.string = "Hello World" + (i + 4);
			sessionStage = sessionStage.thenCompose(s -> s.persist(sequenceId));
		}

		sessionStage = sessionStage.thenCompose(s -> s.flush())
				.thenCompose(v -> openSession());

		for (int i = 0; i < 7; i++) {
			final int id = i + 4;
			sessionStage = sessionStage.thenCompose(s2 ->
					findAndCheck(s2, context, id, id, 0 )
			);
			sessionStage = sessionStage.thenCompose(s2 ->
					findAndCheck(s2, context, id, id, 0 )
			);
			sessionStage = sessionStage.thenCompose(s2 ->
					findCheckIncrementString(s2, context, id, id, id + 1,  0 )
			);
		}

		sessionStage = sessionStage.thenCompose(s -> s.flush());
		for (int i = 0; i < 7; i++) {
			final int id = i + 4;
			sessionStage = sessionStage.thenCompose(s2 ->
					findAndCheck(s2, context, id, id + 1, 1 )
			);
		}

		sessionStage = sessionStage.thenCompose(s3 -> openSession()  );

		for (int i = 0; i < 7; i++) {
			final int id = i + 4;
			sessionStage = sessionStage.thenCompose(s3 ->
					findAndCheck(s3, context, id, id + 1, 1 )
			);
		}

		test( context, sessionStage );
	}

	private CompletionStage<RxSession> findAndCheck(RxSession s, TestContext context, int id, int stringSuffix, int version) {
		return s.find(SequenceId.class, id)
				.thenAccept(bt -> {
					context.assertTrue(bt.isPresent());
					SequenceId bb = bt.get();
					context.assertEquals(bb.id, id);
					context.assertEquals(bb.string, "Hello World" + stringSuffix);
					context.assertEquals(bb.version, version);
				}).thenApply( v-> s );
	}

	private CompletionStage<RxSession> findCheckIncrementString(RxSession s, TestContext context, int id, int stringSuffixOld, int stringSuffixNew, int version) {
		return s.find(SequenceId.class, id)
				.thenAccept(bt -> {
					context.assertTrue(bt.isPresent());
					SequenceId bb = bt.get();
					context.assertEquals(bb.id, id);
					context.assertEquals(bb.string, "Hello World" + stringSuffixOld);
					context.assertEquals(bb.version, version);
					bb.string = "Hello World" + (stringSuffixNew);
				}).thenApply(v -> s);
	}

	@Entity(name = "SequenceId")
	@SequenceGenerator(name = "seq",
			sequenceName = "test_hilo_id_seq",
			initialValue = 2,
			allocationSize = 3
	)
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
