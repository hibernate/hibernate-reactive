package org.hibernate.rx;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.boot.model.naming.ImplicitNamingStrategyLegacyJpaImpl;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.registry.internal.StandardServiceRegistryImpl;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.service.ServiceRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class EagerUniManyToOneAssociationTest {

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
		configuration.setImplicitNamingStrategy( ImplicitNamingStrategyLegacyJpaImpl.INSTANCE );
		configuration.setProperty( AvailableSettings.DIALECT, "org.hibernate.dialect.PostgreSQL9Dialect" );
		configuration.setProperty( AvailableSettings.DRIVER, "org.postgresql.Driver" );
		configuration.setProperty( AvailableSettings.USER, "hibernate-rx" );
		configuration.setProperty( AvailableSettings.PASS, "hibernate-rx" );
		configuration.setProperty( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx" );

		configuration.addAnnotatedClass( Book.class );
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
		dropTable()
				.whenComplete( (res, err) -> {
					try {
						// FIXME: An error here won't cause any test failure.
						//  Probably because async.complete() has been already called.
						context.fail( err );
					}
					finally {
						sessionFactory.close();
					}
				} );
	}

	private CompletionStage<Integer> dropTable() {
		return connection()
				.update( "drop database hibernate-rx if exists;" );
	}

	private RxConnection connection() {
		RxConnectionPoolProvider poolProvider = sessionFactory.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );
		return poolProvider.getConnection();
	}

	@Test
	public void persistOneBook(TestContext context) {
		final Book book = new Book( 6, "The Boy, The Mole, The Fox and The Horse" );
		final Author author = new Author( 5, "Charlie Mackesy", book );

		RxSession rxSession = session.reactive();
		test(
				context,
				rxSession
						.persist( book )
						.thenCompose( v -> rxSession.persist( author ) )
						.thenCompose( v -> rxSession.flush() )
						.thenCompose( v -> rxSession.find( Author.class, author.getId() ) )
						.thenAccept( optionalAuthor -> {
							context.assertTrue( optionalAuthor.isPresent() );
							context.assertEquals( author, optionalAuthor.get() );
							context.assertEquals( book, optionalAuthor.get().getBook()  );
						} )
		);
	}

	@Test
	public void persistTwoAuthors(TestContext context) {
		final Book goodOmens = new Book( 72433, "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch" );
		final Author neilGaiman = new Author( 21421, "Neil Gaiman", goodOmens );
		final Author terryPratchett = new Author( 2111, "Terry Pratchett", goodOmens );

		RxSession rxSession = session.reactive();
		test(
				context,
				rxSession
						.persist( goodOmens )
						.thenCompose( v -> rxSession.persist( terryPratchett ) )
						.thenCompose( v -> rxSession.persist( neilGaiman ) )
						.thenCompose( v -> rxSession.flush() )
						.thenCompose( v -> rxSession.find( Author.class, neilGaiman.getId() ) )
						.thenAccept( optionalAuthor -> {
							context.assertTrue( optionalAuthor.isPresent() );
							context.assertEquals( neilGaiman, optionalAuthor.get() );
							context.assertEquals( goodOmens, optionalAuthor.get().getBook()  );
						} )
		);
	}

	@Entity
	@Table(name = Book.TABLE)
	public static class Book {
		public static final String TABLE = "Book";

		@Id
		private Integer id;
		private String title;

		public Book() {
		}

		public Book(Integer id, String title) {
			this.id = id;
			this.title = title;
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

		@Id
		private Integer id;
		private String name;

		@ManyToOne(fetch = FetchType.EAGER)
		private Book book;

		public Author() {
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
