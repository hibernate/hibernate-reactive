package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.Filter;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;
import org.junit.Test;

import javax.persistence.*;

import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FilterTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		if ( DatabaseConfiguration.dbType() != DBType.DB2 ) {
			configuration.setProperty(AvailableSettings.USE_SQL_COMMENTS, "true" );
		}
		configuration.addPackage(this.getClass().getPackage().getName());
		configuration.addAnnotatedClass(Node.class);
		configuration.addAnnotatedClass(Element.class);
		return configuration;
	}

	@Test
	public void testFilter(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.get(0).deleted = true;

		test(context,
				openSession()
						.thenCompose(s -> s.persist(basik))
						.thenCompose(s -> s.flush())
						.thenCompose(v -> openSession())
						.thenCompose(s -> {
							s.enableFilter("current");
							return s.createQuery("select distinct n from Node n left join fetch n.elements order by n.id")
									.setComment("Hello World!")
									.getResultList();
						})
						.thenAccept(list -> {
							context.assertEquals(list.size(), 2);
							context.assertEquals(((Node) list.get(0)).elements.size(), 2);
						})
						.thenCompose(v -> openSession())
						.thenCompose(s -> {
							s.enableFilter("current");
							return s.createQuery("select distinct n, e from Node n join n.elements e").getResultList();
						})
						.thenAccept(list -> context.assertEquals(list.size(), 2))
		);
	}

	@Entity(name = "Element")
	@Table(name = "Element")
	@Filter(name = "current")
	public static class Element {
		@Id
		@GeneratedValue
		Integer id;

		@ManyToOne
		Node node;

		boolean deleted;

		public Element(Node node) {
			this.node = node;
		}

		Element() {
		}
	}

	@Entity(name = "Node")
	@Table(name = "Node")
	@Filter(name = "current")
	public static class Node {

		@Id
		@GeneratedValue
		Integer id;
		@Version
		Integer version;
		String string;

		boolean deleted;

		@ManyToOne(fetch = FetchType.LAZY,
				cascade = {CascadeType.PERSIST,
						CascadeType.REFRESH,
						CascadeType.MERGE,
						CascadeType.REMOVE})
		Node parent;

		@OneToMany(fetch = FetchType.EAGER,
				cascade = {CascadeType.PERSIST,
						CascadeType.REMOVE},
				mappedBy = "node")
		@Filter(name = "current")
		List<Element> elements = new ArrayList<>();

		@Transient
		boolean prePersisted;
		@Transient
		boolean postPersisted;
		@Transient
		boolean preUpdated;
		@Transient
		boolean postUpdated;
		@Transient
		boolean postRemoved;
		@Transient
		boolean preRemoved;
		@Transient
		boolean loaded;

		public Node(String string) {
			this.string = string;
		}

		Node() {
		}

		@PrePersist
		void prePersist() {
			prePersisted = true;
		}

		@PostPersist
		void postPersist() {
			postPersisted = true;
		}

		@PreUpdate
		void preUpdate() {
			preUpdated = true;
		}

		@PostUpdate
		void postUpdate() {
			postUpdated = true;
		}

		@PreRemove
		void preRemove() {
			preRemoved = true;
		}

		@PostRemove
		void postRemove() {
			postRemoved = true;
		}

		@PostLoad
		void postLoad() {
			loaded = true;
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
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Node node = (Node) o;
			return Objects.equals(string, node.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
