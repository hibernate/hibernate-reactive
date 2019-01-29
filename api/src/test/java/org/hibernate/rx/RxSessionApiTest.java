package org.hibernate.rx;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple tests for checking that the {@link RxSession} API makes sense.
 */
public class RxSessionApiTest {

	private final ConcurrentHashMap<Object, Object> db = new ConcurrentHashMap<>();

	RxSession session = new MockRxSession.Builder<GuineaPig>()
			.find( (type, id) -> type.cast( db.entrySet().stream()
													.filter( e -> e.getKey().equals( id ) )
													.map( Map.Entry::getValue )
													.findAny()
													.orElse( null ) ) )
			.persist( p -> db.put( ( (GuineaPig) p ).getId(), (GuineaPig) p ) )
			.remove( p -> db.remove( ( (GuineaPig) p ).getId() ) ).build();

	@Test
	public void testLoadNull() throws Exception {
		session.find( GuineaPig.class, 1 );

//		assertThat( loadedPig ).isNull();
	}

	@Test
	public void testLoad() throws Exception {
		GuineaPig pig = new GuineaPig( 5, "Hamtaro" );
		db.put( pig.getId(), pig );

		session.find( GuineaPig.class, 5 );

//		assertThat( loadedPig ).isEqualTo( pig );
	}

	@Test
	public void testPersist() throws Exception {
		final GuineaPig pig = new GuineaPig( 1, "Buttercup" );

		session.persist( pig );

//		assertThat( db.values() ).containsExactly( pig );
	}

	@Test
	public void testRemove() throws Exception {
		GuineaPig pig = new GuineaPig( 5, "McCloud" );
		db.put( pig.getId(), pig );

		session.remove( pig );

//		assertThat( db ).isEmpty();
	}

	@Test
	public void testRemoveWithMoreThanOneEntry() throws Exception {
		GuineaPig highlander = new GuineaPig( 15, "Highlander" );
		GuineaPig duncan = new GuineaPig( 82, "Duncan" );

		db.put( highlander.getId(), highlander );
		db.put( duncan.getId(), duncan );

		session.remove( duncan );

//		assertThat( db.values() ).containsExactly( highlander );
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
