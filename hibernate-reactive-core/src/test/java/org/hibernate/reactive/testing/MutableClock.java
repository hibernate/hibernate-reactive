/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.hibernate.generator.internal.CurrentTimestampGeneration;

/**
 * A {@link Clock} that returns the instant set by the test.
 * <p>
 * Use it with {@link CurrentTimestampGeneration#CLOCK_SETTING_NAME} to control the values
 * generated in the JVM for {@code @CreationTimestamp}, {@code @UpdateTimestamp} and {@code @CurrentTimestamp}.
 *
 * @see org.hibernate.annotations.SourceType#VM
 */
public final class MutableClock extends Clock {

	private final ZoneId zoneId;
	private volatile Instant instant;

	public MutableClock(Instant instant) {
		this( ZoneOffset.UTC, instant );
	}

	public MutableClock(ZoneId zoneId, Instant instant) {
		this.zoneId = zoneId;
		this.instant = instant;
	}

	@Override
	public ZoneId getZone() {
		return zoneId;
	}

	@Override
	public Clock withZone(ZoneId zone) {
		return new MutableClock( zone, instant );
	}

	@Override
	public Instant instant() {
		return instant;
	}

	public void setInstant(Instant instant) {
		this.instant = instant;
	}
}
