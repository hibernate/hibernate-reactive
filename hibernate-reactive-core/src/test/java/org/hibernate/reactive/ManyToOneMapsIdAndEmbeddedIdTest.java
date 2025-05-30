/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.EmbeddedColumnNaming;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToMany;

import static org.assertj.core.api.Assertions.assertThat;

public class ManyToOneMapsIdAndEmbeddedIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Link.class, GPSPoint.class, NetPoint.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		NetPointKey startKey = new NetPointKey( 1, NetPointType.STOP_POINT );
		NetPointKey endKey = new NetPointKey( 2, NetPointType.STOP_POINT );
		LinkKey linkKey = new LinkKey( startKey, endKey, "123" );

		final NetPoint start = new NetPoint();
		fillWithBogusValues( start );
		start.key = startKey;

		final NetPoint end = new NetPoint();
		fillWithBogusValues( end );
		end.key = endKey;

		final Link link = new Link();
		link.key = linkKey;
		link.addPoint( new GPSPoint( new GPSPointKey( linkKey, 0 ), link, 1, 1, 1 ) );
		link.addPoint( new GPSPoint( new GPSPointKey( linkKey, 1 ), link, 1, 1, 1 ) );
		link.addPoint( new GPSPoint( new GPSPointKey( linkKey, 2 ), link, 1, 1, 1 ) );

		test(
				context, getMutinySessionFactory().withTransaction( session -> session
						.persistAll( start, end, link ) )
		);
	}

	@Test
	public void test(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.createQuery( "from Link", Link.class ).getResultList() )
				.invoke( links -> assertThat( links ).hasSize( 1 ) )
		);
	}

	void fillWithBogusValues(NetPoint start) {
		start.gpsLatitude = 1;
		start.gpsLongitude = 1;
		start.operatingDepartmentId = "123";
		start.operatingDepartmentShortName = "123 - 123";
	}

	@Entity(name = "GPSPoint")
	public static class GPSPoint {

		@EmbeddedId
		public GPSPointKey key;

		@ManyToOne
		@MapsId("link")
		public Link link;

		@Column(nullable = false)
		public Integer latitude;

		@Column(nullable = false)
		public Integer longitude;

		@Column(nullable = false)
		public Integer distance;

		public GPSPoint() {
		}

		public GPSPoint(GPSPointKey key, Link link, Integer latitude, Integer longitude, Integer distance) {
			this.key = key;
			this.link = link;
			this.latitude = latitude;
			this.longitude = longitude;
			this.distance = distance;
		}
	}

	@Embeddable
	public record GPSPointKey(
			@Embedded
			LinkKey link,

			Integer position
	) {

	}

	@Entity(name = "Link")
	public static class Link {

		@EmbeddedId
		public LinkKey key;

		@OneToMany(mappedBy = "link", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
		public List<GPSPoint> gpsPoints = new ArrayList<>();

		public void addPoint(GPSPoint point) {
			gpsPoints.add( point );
			point.link = this;
		}
	}

	@Embeddable
	public record LinkKey(
			@Embedded
			@EmbeddedColumnNaming("start_%s")
			NetPointKey start,

			@Embedded
			@EmbeddedColumnNaming("end_%s")
			NetPointKey end,

			String operatingDepartmentId
	) {

	}

	@Embeddable
	public record NetPointKey(
			Integer id,

			@Enumerated(EnumType.ORDINAL)
			NetPointType type
	) {

	}

	@Entity(name = "NetPoint")
	public static class NetPoint {

		@EmbeddedId
		public NetPointKey key;

		@Column(nullable = false)
		public String operatingDepartmentId;

		@Column(nullable = false)
		public String operatingDepartmentShortName;

		@Column(nullable = false)
		public Integer gpsLatitude;

		@Column(nullable = false)
		public Integer gpsLongitude;
	}

	public enum NetPointType {
		@Deprecated UNSPECIFIED,
		STOP_POINT,
		DEPOT_POINT,
		BEACON,
		SECTION_POINT
	}
}
