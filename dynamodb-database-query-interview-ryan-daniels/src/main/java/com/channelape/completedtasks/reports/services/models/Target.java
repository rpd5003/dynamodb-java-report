package com.channelape.completedtasks.reports.services.models;

public class Target {

	public static enum Type {
		CHANNEL, SUPPLIER, BUSINESS
	}

	private final Type type;
	private final String id;

	public Target(final Type type, final String id) {
		this.type = type;
		this.id = id;
	}

	public Type getType() {
		return type;
	}

	public String getId() {
		return id;
	}

}
