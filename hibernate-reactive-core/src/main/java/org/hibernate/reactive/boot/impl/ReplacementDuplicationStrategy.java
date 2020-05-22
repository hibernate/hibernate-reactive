package org.hibernate.reactive.boot.impl;

import org.hibernate.event.service.spi.DuplicationStrategy;

/**
 * A {@link DuplicationStrategy} that replaces the default event
 * listeners in Hibernate core with our listeners.
 */
class ReplacementDuplicationStrategy implements DuplicationStrategy {

	public static final DuplicationStrategy INSTANCE = new ReplacementDuplicationStrategy();

	private ReplacementDuplicationStrategy() {}

	@Override
	public boolean areMatch(Object listener, Object original) {
		return listener.getClass().getName().startsWith("org.hibernate.reactive.event.impl")
				&& original.getClass().getName().startsWith("org.hibernate.event.internal");
	}

	@Override
	public Action getAction() {
		return Action.REPLACE_ORIGINAL;
	}
}
