package org.hibernate.rx;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.registry.internal.StandardServiceRegistryImpl;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.service.ServiceRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.persistence.*;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@RunWith(VertxUnitRunner.class)
public class UnionSubclassInheritanceTest {

	@Rule
	public Timeout rule = Timeout.seconds( 3600 );

	RxHibernateSession session = null;
	SessionFactoryImplementor sessionFactory = null;

	private static void test(TestContext context, CompletionStage<?> cs) {
		// this will be added to TestContext in the next vert.x release
		Async async = context.async();
		cs.whenComplete( (res, err) -> {
			if ( err != null ) {
				context.fail( err );
			}
			else {
				async.complete();
			}
		} );
	}

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setProperty( Environment.HBM2DDL_AUTO, "create-drop" );
		configuration.setProperty( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx?user=hibernate-rx&password=hibernate-rx" );
		configuration.setProperty( AvailableSettings.SHOW_SQL, "true" );

		configuration.addAnnotatedClass( Book.class );
		configuration.addAnnotatedClass( SpellBook.class );
		configuration.addAnnotatedClass( Author.class );

		return configuration;
	}

	protected BootstrapServiceRegistry buildBootstrapServiceRegistry() {
		final BootstrapServiceRegistryBuilder builder = new BootstrapServiceRegistryBuilder();
		builder.applyClassLoader( getClass().getClassLoader() );
		return builder.build();
	}

	protected StandardServiceRegistryImpl buildServiceRegistry(
			BootstrapServiceRegistry bootRegistry,
			Configuration configuration) {
		Properties properties = new Properties();
		properties.putAll( configuration.getProperties() );
		ConfigurationHelper.resolvePlaceHolders( properties );

		StandardServiceRegistryBuilder cfgRegistryBuilder = configuration.getStandardServiceRegistryBuilder();
		StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder( bootRegistry, cfgRegistryBuilder.getAggregatedCfgXml() )
				.applySettings( properties );

		return (StandardServiceRegistryImpl) registryBuilder.build();
	}

	@Before
	public void init() {
		// for now, build the configuration to get all the property settings
		Configuration configuration = constructConfiguration();
		BootstrapServiceRegistry bootRegistry = buildBootstrapServiceRegistry();
		ServiceRegistry serviceRegistry = buildServiceRegistry( bootRegistry, configuration );
		// this is done here because Configuration does not currently support 4.0 xsd
		sessionFactory = (SessionFactoryImplementor) configuration.buildSessionFactory( serviceRegistry );
		session = sessionFactory.unwrap( RxHibernateSessionFactory.class ).openRxSession();
	}

	@After
	public void tearDown(TestContext context) {
		sessionFactory.close();
	}

	private CompletionStage<RxSession> session() {
		return RxUtil.completedFuture(session.reactive());
	}

	private Function<Object, RxSession> newSession() {
		return v -> {
			session.close();
			session = sessionFactory.unwrap( RxHibernateSessionFactory.class ).openRxSession();
			return session.reactive();
		};
	}

	@Test
	public void testRootClassViaAssociation(TestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date());
		final Author author = new Author( "Charlie Mackesy", book );

		test(
				context,
				session()
						.thenCompose( s -> s.persist( book ) )
						.thenCompose( s -> s.persist( author ) )
						.thenCompose( s -> s.flush() )
						.thenApply(newSession())
						.thenCompose( s2 -> s2.find( Author.class, author.getId() ) )
						.thenAccept( auth -> {
							context.assertTrue( auth.isPresent() );
							context.assertEquals( author, auth.get() );
							context.assertEquals( book.getTitle(), auth.get().getBook().getTitle()  );
						} )
		);
	}

	@Test
	public void testSubclassViaAssociation(TestContext context) {
		final SpellBook book = new SpellBook( 6, "Necronomicon", true, new Date());
		final Author author = new Author( "Abdul Alhazred", book );

		test(
				context,
				session()
						.thenCompose( s -> s.persist( book ))
						.thenCompose( s -> s.persist( author ) )
						.thenCompose( s -> s.flush() )
						.thenCompose( s -> s.find( Author.class, author.getId() ) )
						.thenAccept( auth -> {
							context.assertTrue( auth.isPresent() );
							context.assertEquals( author, auth.get() );
							context.assertEquals( book.getTitle(), auth.get().getBook().getTitle()  );
						} )
		);
	}

	@Test
	public void testRootClassViaFind(TestContext context) {

		final Book novel = new Book( 6, "The Boy, The Mole, The Fox and The Horse", new Date());
		final Author author = new Author( "Charlie Mackesy", novel );

		test( context,
				session()
						.thenCompose(s -> s.persist(novel))
						.thenCompose(s -> s.persist(author))
						.thenCompose(s -> s.flush())
						.thenApply(newSession())
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept(book -> {
							context.assertTrue(book.isPresent());
							context.assertFalse(book.get() instanceof SpellBook);
							context.assertEquals(book.get().getTitle(), "The Boy, The Mole, The Fox and The Horse");
						}));
	}

	@Test
	public void testSubclassViaFind(TestContext context) {
		final SpellBook spells = new SpellBook( 6, "Necronomicon", true, new Date());
		final Author author = new Author( "Abdul Alhazred", spells );

		test( context,
				session()
						.thenCompose(s -> s.persist(spells))
						.thenCompose(s -> s.persist(author))
						.thenCompose(s -> s.flush())
						.thenApply(newSession())
						.thenCompose(s -> s.find(Book.class, 6))
						.thenAccept(book -> {
							context.assertTrue(book.isPresent());
							context.assertTrue(book.get() instanceof SpellBook);
							context.assertEquals(book.get().getTitle(), "Necronomicon");
						}));
	}

	@Entity
	@Table(name = SpellBook.TABLE)
	@DiscriminatorValue("S")
	public static class SpellBook extends Book {
		public static final String TABLE = "SpellBook";

		private boolean forbidden;

		public SpellBook(Integer id, String title, boolean forbidden, Date published) {
			super(id, title, published);
			this.forbidden = forbidden;
		}

		SpellBook() {}

		public boolean getForbidden() {
			return forbidden;
		}
	}

	@Entity
	@Table(name = Book.TABLE)
	@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
	public static class Book {
		public static final String TABLE = "Book";

		@Id private Integer id;
		private String title;
		@Temporal(TemporalType.DATE)
		private Date published;

		public Book() {
		}

		public Book(Integer id, String title, Date published) {
			this.id = id;
			this.title = title;
			this.published = published;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public Date getPublished() {
			return published;
		}

		public void setPublished(Date published) {
			this.published = published;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Book book = (Book) o;
			return Objects.equals( title, book.title );
		}

		@Override
		public int hashCode() {
			return Objects.hash( title );
		}
	}

	@Entity
	@Table(name = Author.TABLE)
	public static class Author {

		public static final String TABLE = "Author";

		@Id @GeneratedValue
		private Integer id;
		private String name;

		@ManyToOne
		private Book book;

		public Author() {
		}

		public Author(String name, Book book) {
			this.name = name;
			this.book = book;
		}

		public Author(Integer id, String name, Book book) {
			this.id = id;
			this.name = name;
			this.book = book;
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

		public Book getBook() {
			return book;
		}

		public void setBook(Book book) {
			this.book = book;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Author author = (Author) o;
			return Objects.equals( name, author.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
