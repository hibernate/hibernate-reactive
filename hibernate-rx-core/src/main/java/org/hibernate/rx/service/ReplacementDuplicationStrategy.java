package org.hibernate.rx.service;

import org.hibernate.event.service.spi.DuplicationStrategy;

class ReplacementDuplicationStrategy implements DuplicationStrategy {

	public static final DuplicationStrategy INSTANCE = new ReplacementDuplicationStrategy();

	private ReplacementDuplicationStrategy() {}

	@Override
	public boolean areMatch(Object listener, Object original) {
		return listener.getClass().getName().startsWith("org.hibernate.rx.event.impl")
				&& original.getClass().getName().startsWith("org.hibernate.event.internal");
	}

	@Override
	public Action getAction() {
		return Action.REPLACE_ORIGINAL;
	}
}
