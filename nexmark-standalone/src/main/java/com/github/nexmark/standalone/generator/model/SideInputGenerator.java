package com.github.nexmark.standalone.generator;

import java.util.ArrayList;

/**
 * Write data to be read as a side input.
 *
 * <p>Contains pairs of a number and its string representation to model lookups of some enrichment
 * data by id.
 *
 * <p>Generated data covers the range {@code [0, sideInputRowCount)} so lookup joins on any
 * desired id field can be modeled by looking up {@code id % sideInputRowCount}.
 */
public class SideInputGenerator {

	public ArrayList<String> prepareSideInput(int sideInputRowCount) {
		ArrayList<String> result = new ArrayList<>();

		for (int i = 0; i < sideInputRowCount; i++) {
			// Formatting sideinput as JSON to be sent using Kafka
			String jsonFormat = "{\"key\":" + i + ",\"value\":" + i + "}";
			result.add(jsonFormat);
		}

		return result;
	}
}
