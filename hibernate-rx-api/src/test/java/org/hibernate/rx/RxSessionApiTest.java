package org.hibernate.rx;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple tests for checking that the {@link RxSession} API makes sense.
 */
@ExtendWith(VertxExtension.class)
public class RxSessionApiTest {

	private final ConcurrentHashMap<Object, Object> db = new ConcurrentHashMap<>();

	RxSession session = new MockRxSession.Builder<GuineaPig>()
			.find( (type, id) -> type.cast( db.entrySet().stream()
													.filter( e -> e.getKey().equals( id ) )
													.map( Map.Entry::getValue )
													.findAny()
													.orElse( null ) ) )
			.persist( p -> db.put( ( (GuineaPig) p ).getId(), p ) )
			.remove( p -> db.remove( ( (GuineaPig) p ).getId() ) ).build();

	@Test
	public void testLoadNull(VertxTestContext testContext) throws Throwable {
		session.find( GuineaPig.class, 1 )
				.whenComplete( (result, err) -> {
					try {
						assertThat( (Optional) result ).isNotPresent();
						testContext.completeNow();
					}
					catch (Throwable e) {
						testContext.failNow( e );
					}
				} );
	}

	@Test
	public void testLoad(VertxTestContext testContext) throws Exception {
		GuineaPig pig = new GuineaPig( 5, "Hamtaro" );
		db.put( pig.getId(), pig );

		session.find( GuineaPig.class, pig.getId() )
				.whenComplete( (result, err) -> {
					try {
						assertThat( (Optional) result ).isPresent().hasValue( pig );
						testContext.completeNow();
					}
					catch (Throwable e) {
						testContext.failNow( e );
					}
				} );
	}

	@Test
	public void testPersist(VertxTestContext testContext) throws Exception {
		final GuineaPig pig = new GuineaPig( 1, "Buttercup" );
		session.persist( pig )
				.whenComplete( (result, err) -> {
					try {
						assertThat( db.values() ).containsExactly( pig );
						testContext.completeNow();
					}
					catch (Throwable e) {
						testContext.failNow( e );
					}
				} );
	}

	@Test
	public void testRemove(VertxTestContext testContext) throws Exception {
		GuineaPig pig = new GuineaPig( 5, "McCloud" );
		db.put( pig.getId(), pig );

		session.remove( pig )
				.whenComplete( (result, err) -> {
					try {
						assertThat( db ).isEmpty();
						testContext.completeNow();
					}
					catch (Throwable e) {
						testContext.failNow( e );
					}
				} );
	}

	@Test
	public void testRemoveWithMoreThanOneEntry(VertxTestContext testContext) throws Exception {
		GuineaPig highlander = new GuineaPig( 15, "Highlander" );
		GuineaPig duncan = new GuineaPig( 82, "Duncan" );

		db.put( highlander.getId(), highlander );
		db.put( duncan.getId(), duncan );

		session.remove( duncan )
				.whenComplete( (result, err) -> {
					try {
						assertThat( db.values() ).containsExactly( highlander );
						testContext.completeNow();
					}
					catch (Throwable e) {
						testContext.failNow( e );
					}
				} );
	}

	public static class GuineaPig {
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(String name) {
			this.name = name;
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
