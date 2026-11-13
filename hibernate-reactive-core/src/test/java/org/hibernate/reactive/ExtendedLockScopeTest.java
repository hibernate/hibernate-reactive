/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Locking;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.internal.StageSessionImpl;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.PessimisticLockScope;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * {@link PessimisticLockScope#EXTENDED} also locks the rows of the element collections
 * and of the join tables owned by the entity.
 * <p>
 * Hibernate Reactive doesn't expose the lock scope in {@link Stage}: the tests set it
 * on the {@link LockOptions} used with the {@link ReactiveSession}.
 * </p>
 */
@Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
public class ExtendedLockScopeTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;

	private static final Author KING = new Author( 1, "Stephen King" );
	private static final Author STRAUB = new Author( 2, "Peter Straub" );
	private static final Book TALISMAN = new Book( 1, "The Talisman", Set.of( "Dark fantasy", "Adventure" ), Set.of( KING, STRAUB ) );
	private static final Book OTHER = new Book( 2, "The Dark Tower", Set.of( "Western" ), Set.of( KING ) );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class, Author.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		final Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( ExtendedLockScopeTest::selectQueryFilter, configuration.getProperties() );
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean selectQueryFilter(String s) {
		final String sql = s.toLowerCase( Locale.ROOT );
		return sql.startsWith( "select " ) || sql.startsWith( "update lock_scope_book set" );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		// Persist copies: the expected instances are only used for the assertions
		final Author king = new Author( KING.id, KING.name );
		final Author straub = new Author( STRAUB.id, STRAUB.name );
		final Book talisman = new Book( TALISMAN.id, TALISMAN.title, TALISMAN.tags, Set.of( king, straub ) );
		final Book other = new Book( OTHER.id, OTHER.title, OTHER.tags, Set.of( king ) );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( king, straub, talisman, other ) )
				.thenAccept( v -> sqlTracker.clear() ) );
	}

	private static LockOptions lockOptions(PessimisticLockScope scope, Locking.FollowOn followOn) {
		return new LockOptions( LockMode.PESSIMISTIC_WRITE )
				.setScope( scope )
				.setFollowOnStrategy( followOn );
	}

	private static ReactiveSession reactive(Stage.Session session) {
		return ( (StageSessionImpl) session ).unwrap( ReactiveSession.class );
	}

	private static CompletionStage<Book> find(Stage.Session session, LockOptions lockOptions) {
		return reactive( session )
				.reactiveFind( Book.class, TALISMAN.id, lockOptions, null );
	}

	private static CompletionStage<Book> query(Stage.Session session, LockOptions lockOptions) {
		return reactive( session )
				.createReactiveQuery( "from Book b where b.id = :id", Book.class )
				.setParameter( "id", TALISMAN.id )
				.setLockOptions( lockOptions )
				.getReactiveSingleResult();
	}

	@Test
	public void testFindNormalScopeDoesNotLockCollectionTables(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.NORMAL, Locking.FollowOn.ALLOW ) )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).isEmpty();
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).isEmpty();
				} ) ) );
	}

	@Test
	public void testFindExtendedScope(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
				} ) ) );
	}

	@Test
	public void testQueryExtendedScope(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> query( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
				} ) ) );
	}

	// Covers the locking of the collection tables when it's done after the follow-on locking of the entity
	@Test
	public void testFindExtendedScopeWithFollowOnLocking(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.FORCE ) )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
				} ) ) );
	}

	// Covers the FETCHED scope, that only locks the collections loaded by the query
	@Test
	public void testFetchedScopeWithFollowOnLocking(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> reactive( s )
				.createReactiveQuery( "from Book b left join fetch b.tags where b.id = :id", Book.class )
				.setParameter( "id", TALISMAN.id )
				.setLockOptions( lockOptions( PessimisticLockScope.FETCHED, Locking.FollowOn.FORCE ) )
				.getReactiveSingleResult()
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).isEmpty();
				} ) ) );
	}

	// Covers PESSIMISTIC_FORCE_INCREMENT, that increments the version of the entity as well
	@Test
	public void testFindExtendedScopeForceIncrement(VertxTestContext context) {
		final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT ).setScope( PessimisticLockScope.EXTENDED );
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( found.version ).isEqualTo( 1 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
					// The version is incremented after the collection tables are locked
					final List<String> queries = sqlTracker.getLoggedQueries();
					assertThat( queries.get( queries.size() - 1 ) ).startsWithIgnoringCase( "update lock_scope_book set version" );
				} ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( Book.class, TALISMAN.id ) ) )
				.thenAccept( found -> assertThat( found.version ).isEqualTo( 1 ) ) );
	}

	// Covers queries, where the version is incremented after the collection tables are locked
	@Test
	public void testQueryExtendedScopeForceIncrement(VertxTestContext context) {
		final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT ).setScope( PessimisticLockScope.EXTENDED );
		test( context, getSessionFactory().withTransaction( s -> query( s, lockOptions )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( found.version ).isEqualTo( 1 );
					assertThat( s.getLockMode( found ) ).isEqualTo( LockMode.PESSIMISTIC_FORCE_INCREMENT );
					final List<String> queries = sqlTracker.getLoggedQueries();
					assertThat( queries ).hasSize( 4 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
					assertThat( queries.get( 3 ) ).startsWithIgnoringCase( "update lock_scope_book set version" );
				} ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( Book.class, TALISMAN.id ) ) )
				.thenAccept( found -> assertThat( found.version ).isEqualTo( 1 ) ) );
	}

	// Covers an entity already in the session: find doesn't increment its version again
	@Test
	public void testFindForceIncrementAfterQueryExtendedScope(VertxTestContext context) {
		final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT ).setScope( PessimisticLockScope.EXTENDED );
		test( context, getSessionFactory().withTransaction( s -> query( s, lockOptions )
				.thenCompose( v -> s.find( Book.class, TALISMAN.id, LockMode.PESSIMISTIC_FORCE_INCREMENT ) )
				.thenCompose( v -> s.find( Book.class, TALISMAN.id, LockMode.PESSIMISTIC_FORCE_INCREMENT ) )
				.thenAccept( found -> assertThat( found.version ).isEqualTo( 1 ) ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( Book.class, TALISMAN.id ) ) )
				.thenAccept( found -> assertThat( found.version ).isEqualTo( 1 ) ) );
	}

	// Covers follow-on locking, where the version is incremented once after the entity is loaded
	@Test
	public void testFindExtendedScopeForceIncrementWithFollowOnLocking(VertxTestContext context) {
		final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT )
				.setScope( PessimisticLockScope.EXTENDED )
				.setFollowOnStrategy( Locking.FollowOn.FORCE );
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( found.version ).isEqualTo( 1 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
					final List<String> queries = sqlTracker.getLoggedQueries();
					assertThat( queries.get( queries.size() - 1 ) ).startsWithIgnoringCase( "update lock_scope_book set version" );
				} ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( Book.class, TALISMAN.id ) ) )
				.thenAccept( found -> assertThat( found.version ).isEqualTo( 1 ) ) );
	}

	private static CompletionStage<Book> findAndLock(Stage.Session session, LockOptions lockOptions) {
		return session.find( Book.class, TALISMAN.id )
				.thenCompose( book -> {
					sqlTracker.clear();
					// Stage.Session doesn't expose lock(Object, LockOptions)
					return ( (StageSessionImpl) session ).lock( book, lockOptions )
							.thenApply( v -> book );
				} );
	}

	@Test
	public void testLockNormalScopeDoesNotLockCollectionTables(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> findAndLock( s, lockOptions( PessimisticLockScope.NORMAL, Locking.FollowOn.ALLOW ) )
				.thenAccept( book -> {
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).isEmpty();
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).isEmpty();
				} ) ) );
	}

	@Test
	public void testLockExtendedScope(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> findAndLock( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) )
				.thenAccept( book -> {
					assertThat( s.getLockMode( book ) ).isEqualTo( LockMode.PESSIMISTIC_WRITE );
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 3 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
				} ) ) );
	}

	// Covers an entity that is already locked: the extended scope still locks the collection tables
	@Test
	public void testLockExtendedScopeAfterNormalScope(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.NORMAL, Locking.FollowOn.ALLOW ) )
				.thenCompose( book -> {
					sqlTracker.clear();
					return ( (StageSessionImpl) s ).lock( book, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) );
				} )
				.thenAccept( v -> {
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
				} ) ) );
	}

	@Test
	public void testLockExtendedScopeLocksCollectionRows(VertxTestContext context) {
		assumeThat( dbType() ).isIn( POSTGRESQL, MYSQL, MARIA );
		test( context, getSessionFactory().withTransaction( s -> findAndLock( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) )
				.thenCompose( book -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) )
				.thenCompose( v -> lockedRows( "select author_id from lock_scope_book_authors where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) )
				.thenCompose( v -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", OTHER.id )
						.thenAccept( rows -> assertThat( rows ).containsExactly( "Western" ) ) ) ) );
	}

	@Test
	public void testLockNormalScopeDoesNotLockCollectionRows(VertxTestContext context) {
		assumeThat( dbType() ).isIn( POSTGRESQL, MYSQL, MARIA );
		test( context, getSessionFactory().withTransaction( s -> findAndLock( s, lockOptions( PessimisticLockScope.NORMAL, Locking.FollowOn.ALLOW ) )
				.thenCompose( book -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).containsExactlyInAnyOrder( "Dark fantasy", "Adventure" ) ) ) ) );
	}

	// Covers PESSIMISTIC_FORCE_INCREMENT: the version is incremented after the collection tables are locked
	@Test
	public void testLockExtendedScopeForceIncrement(VertxTestContext context) {
		final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT ).setScope( PessimisticLockScope.EXTENDED );
		test( context, getSessionFactory().withTransaction( s -> findAndLock( s, lockOptions )
				.thenAccept( book -> {
					assertThat( book.version ).isEqualTo( 1 );
					assertThat( s.getLockMode( book ) ).isEqualTo( LockMode.PESSIMISTIC_FORCE_INCREMENT );
					final List<String> queries = sqlTracker.getLoggedQueries();
					assertThat( queries ).hasSize( 4 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
					assertThat( queries.get( 3 ) ).startsWithIgnoringCase( "update lock_scope_book set version" );
				} ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( Book.class, TALISMAN.id ) ) )
				.thenAccept( found -> assertThat( found.version ).isEqualTo( 1 ) ) );
	}

	// Covers an entity whose version has already been incremented: it is not incremented again
	@Test
	public void testLockExtendedScopeForceIncrementTwice(VertxTestContext context) {
		final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT );
		final LockOptions extendedLockOptions = new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT ).setScope( PessimisticLockScope.EXTENDED );
		test( context, getSessionFactory().withTransaction( s -> findAndLock( s, lockOptions )
				.thenCompose( book -> {
					sqlTracker.clear();
					return ( (StageSessionImpl) s ).lock( book, extendedLockOptions ).thenApply( v -> book );
				} )
				.thenAccept( book -> {
					assertThat( book.version ).isEqualTo( 1 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
					assertThat( sqlTracker.getLoggedQueries() ).noneMatch( sql -> sql.toLowerCase( Locale.ROOT ).startsWith( "update" ) );
				} ) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s.find( Book.class, TALISMAN.id ) ) )
				.thenAccept( found -> assertThat( found.version ).isEqualTo( 1 ) ) );
	}

	// Covers find of an entity already in the session with the lock mode: the extended scope still locks the collection tables
	@Test
	public void testFindExtendedScopeWhenAlreadyLocked(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.NORMAL, Locking.FollowOn.ALLOW ) )
				.thenCompose( book -> {
					sqlTracker.clear();
					return find( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) );
				} )
				.thenAccept( found -> {
					assertThat( found ).isEqualTo( TALISMAN );
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 3 );
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
				} ) ) );
	}

	@Test
	public void testFindExtendedScopeWhenAlreadyLockedLocksCollectionRows(VertxTestContext context) {
		assumeThat( dbType() ).isIn( POSTGRESQL, MYSQL, MARIA );
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.NORMAL, Locking.FollowOn.ALLOW ) )
				.thenCompose( book -> find( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) ) )
				.thenCompose( found -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) )
				.thenCompose( v -> lockedRows( "select author_id from lock_scope_book_authors where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) ) ) );
	}

	@Test
	public void testRefreshExtendedScope(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> s.find( Book.class, TALISMAN.id )
				.thenCompose( book -> {
					sqlTracker.clear();
					return reactive( s )
							.reactiveRefresh( book, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) );
				} )
				.thenAccept( v -> {
					assertThat( lockingQueries( "lock_scope_book_tags" ) ).hasSize( 1 );
					assertThat( lockingQueries( "lock_scope_book_authors" ) ).hasSize( 1 );
				} ) ) );
	}

	@Test
	public void testFindExtendedScopeLocksCollectionRows(VertxTestContext context) {
		assumeThat( dbType() ).isIn( POSTGRESQL, MYSQL, MARIA );
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.ALLOW ) )
				// another transaction can't lock the rows of the collection tables, but it can lock the rows of other entities
				.thenCompose( found -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) )
				.thenCompose( v -> lockedRows( "select author_id from lock_scope_book_authors where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) )
				.thenCompose( v -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", OTHER.id )
						.thenAccept( rows -> assertThat( rows ).containsExactly( "Western" ) ) ) ) );
	}

	@Test
	public void testFindNormalScopeDoesNotLockCollectionRows(VertxTestContext context) {
		assumeThat( dbType() ).isIn( POSTGRESQL, MYSQL, MARIA );
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.NORMAL, Locking.FollowOn.ALLOW ) )
				.thenCompose( found -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).containsExactlyInAnyOrder( "Dark fantasy", "Adventure" ) ) ) ) );
	}

	@Test
	public void testFindExtendedScopeWithFollowOnLockingLocksCollectionRows(VertxTestContext context) {
		assumeThat( dbType() ).isIn( POSTGRESQL, MYSQL, MARIA );
		test( context, getSessionFactory().withTransaction( s -> find( s, lockOptions( PessimisticLockScope.EXTENDED, Locking.FollowOn.FORCE ) )
				.thenCompose( found -> lockedRows( "select tag from lock_scope_book_tags where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) )
				.thenCompose( v -> lockedRows( "select author_id from lock_scope_book_authors where book_id = ", TALISMAN.id )
						.thenAccept( rows -> assertThat( rows ).isEmpty() ) ) ) );
	}

	/**
	 * Tries to lock the rows in another transaction, skipping the ones that are already locked:
	 * the result contains the rows that aren't locked by the current transaction.
	 */
	private static CompletionStage<List<Object>> lockedRows(String select, Object id) {
		// The factory would reuse the session of the current context: we need another connection
		return getSessionFactory().openSession().thenCompose( other -> other
				.withTransaction( t -> other
						.createNativeQuery( select + id + " for update skip locked", Object.class )
						.getResultList() )
				.whenComplete( (rows, e) -> other.close() ) );
	}

	private static List<String> lockingQueries(String table) {
		return sqlTracker.getLoggedQueries().stream()
				.map( s -> s.toLowerCase( Locale.ROOT ) )
				.filter( s -> s.contains( table ) && containsLock( s ) )
				.toList();
	}

	private static boolean containsLock(String sql) {
		return sql.contains( "for update" )
				|| sql.contains( "for no key update" )
				|| sql.contains( "for share" )
				|| sql.contains( "for key share" )
				|| sql.contains( "updlock" )
				|| sql.contains( "use and keep update locks" )
				|| sql.contains( "lock in share mode" );
	}

	@Entity(name = "Book")
	@Table(name = "lock_scope_book")
	public static class Book {
		@Id
		private Integer id;
		@Version
		private int version;
		private String title;
		@ElementCollection
		@CollectionTable(name = "lock_scope_book_tags", joinColumns = @JoinColumn(name = "book_id"))
		@Column(name = "tag")
		private Set<String> tags = new HashSet<>();
		@ManyToMany
		@JoinTable(name = "lock_scope_book_authors", joinColumns = @JoinColumn(name = "book_id"), inverseJoinColumns = @JoinColumn(name = "author_id"))
		private Set<Author> authors = new HashSet<>();

		public Book() {
		}

		public Book(Integer id, String title, Set<String> tags, Set<Author> authors) {
			this.id = id;
			this.title = title;
			this.tags = new HashSet<>( tags );
			this.authors = new HashSet<>( authors );
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			final Book book = (Book) o;
			return Objects.equals( title, book.title );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( title );
		}
	}

	@Entity(name = "Author")
	@Table(name = "lock_scope_author")
	public static class Author {
		@Id
		private Integer id;
		private String name;

		public Author() {
		}

		public Author(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			final Author author = (Author) o;
			return Objects.equals( name, author.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}
}
