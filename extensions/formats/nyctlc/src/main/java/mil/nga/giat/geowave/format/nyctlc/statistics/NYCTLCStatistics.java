package mil.nga.giat.geowave.format.nyctlc.statistics;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.format.nyctlc.NYCTLCUtils;
import net.sf.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by geowave on 4/28/16.
 */
public class NYCTLCStatistics implements
		Mergeable
{

	private long numEntries = 0;

	private List<BaseStat> statsList = new ArrayList<BaseStat>();

	public NYCTLCStatistics() {
		for (NYCTLCUtils.Field field : NYCTLCUtils.Field.values()) {
			if (field.getStatBuilder() != null) {
				statsList.add(field.getStatBuilder().build());
			}
		}
	}

	public void updateStats(
			final SimpleFeature entry ) {
		numEntries++;

		int statIdx = 0;
		// update all of our desired stats as defined in the field enum
		final NYCTLCUtils.Field[] fields = NYCTLCUtils.Field.values();
		for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
			if (fields[fieldIdx].getStatBuilder() != null) {
				if (entry.getAttribute(fields[fieldIdx].getIndexedName()) != null) {
					statsList.get(
							statIdx).updateStat(
							entry.getAttribute(fields[fieldIdx].getIndexedName()));
				}
				statIdx++;
			}
		}
	}

	@Override
	public void merge(
			Mergeable merge ) {
		if ((merge != null) && (merge instanceof NYCTLCStatistics)) {
			NYCTLCStatistics stats = (NYCTLCStatistics) merge;
			numEntries += stats.numEntries;

			for (int statIdx = 0; statIdx < statsList.size(); statIdx++) {
				statsList.get(
						statIdx).merge(
						stats.statsList.get(statIdx));
			}
		}
	}

	@Override
	public byte[] toBinary() {

		byte[][] statsBytes = new byte[statsList.size()][];
		int byteLen = 0;
		for (int statIdx = 0; statIdx < statsList.size(); statIdx++) {
			statsBytes[statIdx] = statsList.get(
					statIdx).toBinary();
			byteLen += statsBytes[statIdx].length + 4;
		}

		final ByteBuffer buffer = ByteBuffer.allocate(8 + byteLen);
		buffer.putLong(numEntries);
		for (int statIdx = 0; statIdx < statsList.size(); statIdx++) {
			buffer.putInt(statsBytes[statIdx].length);
			buffer.put(statsBytes[statIdx]);
		}
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		numEntries = buffer.getLong();
		statsList.clear();
		for (NYCTLCUtils.Field field : NYCTLCUtils.Field.values()) {
			if (field.getStatBuilder() != null) {
				int statLen = buffer.getInt();
				BaseStat stat = field.getStatBuilder().build();
				byte[] statBytes = new byte[statLen];
				buffer.get(statBytes);
				stat.fromBinary(statBytes);
				statsList.add(stat);
			}
		}
	}

	public JSONObject toJSONObject() {
		final JSONObject statsJson = new JSONObject();

		statsJson.put(
				"count",
				numEntries);

		int statIdx = 0;
		final NYCTLCUtils.Field[] fields = NYCTLCUtils.Field.values();
		for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
			if (fields[fieldIdx].getStatBuilder() != null) {
				statsJson.put(
						fields[fieldIdx].getIndexedName(),
						statsList.get(
								statIdx++).toJSONObject());
			}
		}

		return statsJson;
	}

	public static abstract class BaseStat<T> implements
			Mergeable
	{
		public abstract void updateStat(
				T value );

		public abstract void clear();

		public abstract JSONObject toJSONObject();

		public static abstract class BaseStatBuilder
		{
			public abstract BaseStat build();
		}
	}

	public static class MinMaxTotStat extends
			BaseStat<Number>
	{
		double minValue = Double.MAX_VALUE;
		double maxValue = -Double.MAX_VALUE;
		double totalValue = 0;
		long numValues = 0;

		@Override
		public void updateStat(
				Number value ) {
			Double doubleValue = value.doubleValue();
			if (doubleValue < minValue) minValue = doubleValue;
			if (doubleValue > maxValue) maxValue = doubleValue;
			totalValue += doubleValue;
			numValues++;
		}

		public void clear() {
			minValue = Double.MAX_VALUE;
			maxValue = -Double.MAX_VALUE;
			totalValue = 0;
			numValues = 0;
		}

		@Override
		public JSONObject toJSONObject() {
			final JSONObject jsonObj = new JSONObject();
			jsonObj.put(
					"min",
					minValue);
			jsonObj.put(
					"max",
					maxValue);
			jsonObj.put(
					"total",
					totalValue);
			jsonObj.put(
					"avg",
					totalValue / (double) numValues);
			return jsonObj;
		}

		@Override
		public void merge(
				Mergeable merge ) {
			if ((merge != null) && (merge instanceof MinMaxTotStat)) {
				MinMaxTotStat stat = (MinMaxTotStat) merge;
				if (stat.minValue < minValue) minValue = stat.minValue;
				if (stat.maxValue > maxValue) maxValue = stat.maxValue;
				totalValue += stat.totalValue;
				numValues += stat.numValues;
			}
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buffer = ByteBuffer.allocate(32);
			buffer.putDouble(minValue);
			buffer.putDouble(maxValue);
			buffer.putDouble(totalValue);
			buffer.putLong(numValues);
			return buffer.array();
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			minValue = buffer.getDouble();
			maxValue = buffer.getDouble();
			totalValue = buffer.getDouble();
			numValues = buffer.getLong();
		}

		public static class MinMaxTotStatBuilder extends
				BaseStatBuilder
		{
			@Override
			public MinMaxTotStat build() {
				return new MinMaxTotStat();
			}
		}
	}

	public static class IntHistStat extends
			BaseStat<Integer>
	{
		int firstBin;
		int lastBin;
		long[] counts;

		public IntHistStat(
				int firstBin,
				int lastBin ) {
			this.firstBin = firstBin;
			this.lastBin = lastBin;
			this.counts = new long[lastBin - firstBin + 1];
			for (int bin = firstBin; bin <= lastBin; bin++)
				this.counts[bin - this.firstBin] = 0;
		}

		@Override
		public void updateStat(
				Integer bin ) {
			counts[bin - firstBin]++;
		}

		@Override
		public void clear() {
			for (int bin = firstBin; bin <= lastBin; bin++)
				this.counts[bin - firstBin] = 0;
		}

		@Override
		public JSONObject toJSONObject() {
			JSONObject jsonObj = new JSONObject();
			for (int bin = firstBin; bin <= lastBin; bin++)
				jsonObj.put(
						bin,
						counts[bin - firstBin]);
			return jsonObj;
		}

		@Override
		public void merge(
				Mergeable merge ) {
			if ((merge != null) && (merge instanceof IntHistStat)) {
				IntHistStat stat = (IntHistStat) merge;
				for (int bin = firstBin; bin <= lastBin; bin++)
					this.counts[bin - firstBin] += stat.counts[bin - firstBin];
			}
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buffer = ByteBuffer.allocate(8 + counts.length * 8);
			buffer.putInt(firstBin);
			buffer.putInt(lastBin);
			for (int bin = firstBin; bin <= lastBin; bin++)
				buffer.putLong(this.counts[bin - firstBin]);
			return buffer.array();
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			firstBin = buffer.getInt();
			lastBin = buffer.getInt();
			this.counts = new long[lastBin - firstBin + 1];
			for (int bin = firstBin; bin <= lastBin; bin++)
				this.counts[bin - firstBin] = buffer.getLong();
		}

		public static class IntHistStatBuilder extends
				BaseStatBuilder
		{
			int firstBin;
			int lastBin;

			public IntHistStatBuilder(
					int firstBin,
					int lastBin ) {
				this.firstBin = firstBin;
				this.lastBin = lastBin;
			}

			@Override
			public IntHistStat build() {
				return new IntHistStat(
						firstBin,
						lastBin);
			}
		}
	}
}
