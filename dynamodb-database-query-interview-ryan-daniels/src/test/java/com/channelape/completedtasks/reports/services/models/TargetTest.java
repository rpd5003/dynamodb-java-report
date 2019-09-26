package com.channelape.completedtasks.reports.services.models;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.channelape.completedtasks.reports.services.models.Target;

public class TargetTest {

	private static final String SOME_ID = UUID.randomUUID().toString();

	@Test
	public void givenBusinessTypeAndSomeIdWhenCreatingTargetThenReturnTargetWithCorrectTypeAndId() {
		final Target.Type expectedType = Target.Type.BUSINESS;
		final Target actualTarget = new Target(expectedType, SOME_ID);

		assertTarget(expectedType, actualTarget);
	}

	@Test
	public void givenSupplierTypeAndSomeIdWhenCreatingTargetThenReturnTargetWithCorrectTypeAndId() {
		final Target.Type expectedType = Target.Type.SUPPLIER;
		final Target actualTarget = new Target(expectedType, SOME_ID);

		assertTarget(expectedType, actualTarget);
	}

	@Test
	public void givenChannelTypeAndSomeIdWhenCreatingTargetThenReturnTargetWithCorrectTypeAndId() {
		final Target.Type expectedType = Target.Type.CHANNEL;
		final Target actualTarget = new Target(expectedType, SOME_ID);

		assertTarget(expectedType, actualTarget);
	}

	private void assertTarget(final Target.Type expectedType, final Target actualTarget) {
		assertEquals(expectedType, actualTarget.getType());
		assertEquals(SOME_ID, actualTarget.getId());
	}

}
