package com.metamx.druid.aggregation;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntShortHashMap;
import gnu.trove.procedure.TIntByteProcedure;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Calendar;

import com.google.common.hash.Hashing;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ComplexMetricSelector;
import com.metamx.druid.processing.ObjectColumnSelector;
import com.metamx.druid.processing.TimestampColumnSelector;

public class FrequencyCapBufferAggregator implements BufferAggregator {

	private final ObjectColumnSelector uidSelector;
	private final TimestampColumnSelector timestampSelector;
	private final ObjectColumnSelector countSelector;
	private final ObjectColumnSelector bucketIdSelector;
	private final ObjectColumnSelector uuhllSelector;
	private final String type;
	private final short fre;
	private static final Logger log = new Logger(
	        FrequencyCapBufferAggregator.class);

	// public static final int bucketSize = (int) Math.pow(2, 22);

	public FrequencyCapBufferAggregator(ObjectColumnSelector uidSelector,
	        TimestampColumnSelector timestamp, ObjectColumnSelector bucketId,
	        ObjectColumnSelector count, ObjectColumnSelector uuhll,
	        String type, short fre) {
		this.uidSelector = uidSelector;
		this.timestampSelector = timestamp;
		this.countSelector = count;
		this.type = type;
		this.fre = fre;
		this.uuhllSelector = uuhll;
		this.bucketIdSelector = bucketId;

	}

	// hll , count if no cap
	// uid, daykey, hll , count if with cap
	@Override
	public void init(ByteBuffer buf, int position) {
		// log.info(" init pos:"+position);
		// log.info("init me");
		// for (int i = 0; i < bucketSize; i++) {
		// buf.putShort(position + i, (short) 0);
		// }
		// log.info("start init pos:"+buf.position());
		int currentPos = position;
		// if (!type.equals("none")) {
		buf.putLong(currentPos, 0);
		buf.putInt(currentPos + Long.SIZE / 8, 0);
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			buf.putInt(
			        currentPos
			                + (Long.SIZE / 8 + Integer.SIZE / 8 + i
			                        * Integer.SIZE / 8), 0);
		}

		currentPos = position + Long.SIZE / 8 + (FrequencyCapAggregatorFactory.BUCKET_SIZE+1) * Integer.SIZE / 8;
		// }

		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			for (int j = 0; j < HllAggregator.m; j++) {
				buf.put(currentPos + j + (i * HllAggregator.m), (byte) 0);
			}
		}
		currentPos += FrequencyCapAggregatorFactory.BUCKET_SIZE
		        * HllAggregator.m;
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			buf.putLong((currentPos + i * Long.SIZE / 8), 0);
		}
	}

	private void aggHll(ByteBuffer buf, int position, TIntByteHashMap hllObj) {
		// final ByteBuffer fb = buf;
		// final int fp = position;
		int keys[] = hllObj.keys();
		for (int i = 0; i < keys.length; i++) {
			if (hllObj.get(keys[i]) > buf.get(position + keys[i])) {
				// log.info("put value:" +
				// hllObj.get(keys[i])+"put position:"+position);
				buf.put(position + keys[i], hllObj.get(keys[i]));
			}
		}
		// hllObj.forEachEntry(new TIntByteProcedure() {
		// public boolean execute(int a, byte b) {
		// if (b > fb.get(fp + a)) {
		// fb.put(fp + a, b);
		// }
		// return true;
		// }
		// });
	}

	// private int getBucketNum(String key) {
	// int id = Hashing.murmur3_32().hashString(key).asInt();
	// return (int) (id >>> (Long.SIZE - 22));
	// }

	@Override
	public void aggregate(ByteBuffer buf, int position) {
		// log.info(" agg pos:"+position);
		// log.info("start agg pos:"+buf.position());
		Long uid = Long.parseLong(uidSelector.get().toString());
		long timestamp = timestampSelector.getTimestamp();
		Float countOrignal = Float.parseFloat(countSelector.get().toString());
		int bucketId = Integer.parseInt(bucketIdSelector.get().toString());
		TIntByteHashMap hllObj = (TIntByteHashMap) (uuhllSelector.get());

		long count = countOrignal.longValue();

		// int countPosition = FrequencyCapAggregatorFactory.BUCKET_SIZE
		// * HllAggregator.m;
		// if (type.equals("none")) {
		int currentPos = position;
		int currentValue = 0;
		long lastUid = 0;
		int lastTimeKey = 0;

		// if (!type.equals("none")) {
		lastUid = buf.getLong(currentPos);
		lastTimeKey = buf.getInt(currentPos + Long.SIZE / 8);

		currentPos = position + Long.SIZE / 8
		        + (FrequencyCapAggregatorFactory.BUCKET_SIZE + 1)
		        * Integer.SIZE / 8;
		// }
		// log.info("orignal hll:"+hllObj);
		int keys[] = hllObj.keys();
		for (int i = bucketId; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			// aggHll(buf, currentPos + i * HllAggregator.m, hllObj);

			for (int j = 0; j < keys.length; j++) {
				// log.info("currentPos:"+(currentPos+i *
				// HllAggregator.m+keys[j])+" j value :"+j);
				if (hllObj.get(keys[j]) > buf.get((currentPos + i
				        * HllAggregator.m + keys[j]))) {

					// log.info("put hll value:" +
					// hllObj.get(keys[j])+"put position:"+ ((currentPos + i *
					// HllAggregator.m+keys[j])-position));
					buf.put(currentPos + i * HllAggregator.m + keys[j],
					        hllObj.get(keys[j]));
				}
			}
		}
		// log.info("orignal hll:"+hllObj);
		currentPos += FrequencyCapAggregatorFactory.BUCKET_SIZE
		        * HllAggregator.m;

		if (type.equals("none")) {
			for (int i = bucketId; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
				// log.info("put count value:" + count+"put position:"+
				// ((currentPos + i * Long.SIZE / 8)-position));
				buf.putLong(currentPos + i * Long.SIZE / 8,
				        buf.getLong(currentPos + i * Long.SIZE / 8) + count);
			}
		} else {
			int timeKey = 0;
			boolean isNewUser = false;
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis((timestamp));
			if (type.equals("day"))
				timeKey = cal.get(Calendar.DAY_OF_YEAR);
			if (type.equals("week"))
				timeKey = cal.get(Calendar.WEEK_OF_YEAR);
			if (type.equals("month"))
				timeKey = cal.get(Calendar.MONTH);
			if (lastUid != uid || lastTimeKey != timeKey) {
				// log.info("1111 uid:" + uid + "lastuid:"
				// +
				// lastUid+"lastTimeKey"+lastTimeKey+"timeKey"+timeKey);
				buf.putLong(position, uid);
				buf.putInt(position + Long.SIZE / 8, timeKey);
				currentValue = 0;
				isNewUser = true;
			}
			long finalValue = 0;
			for (int i = bucketId; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
				if (!isNewUser) {
					currentValue = buf.getInt(position + Long.SIZE / 8
					        + (i + 1) * Integer.SIZE / 8);
				}
				if ((long) currentValue + count > fre) {

					if (fre - currentValue > 0) {
						// log.info("1111 count:" + count + "currentvalue:"
						// +
						// currentValue);
						finalValue = fre - currentValue;
						buf.putInt(position
						        + (Long.SIZE / 8 + (i + 1) * Integer.SIZE / 8),
						        fre);
					}

				} else {
					// log.info("222 count:" + count + "currentvalue:"
					// +
					// currentValue);
					// log.info("2222bucket num:" + bucketNum + "value num:"
					// + (shortMutationBuffer.get(bucketNum) + 1));
					finalValue = count;
					buf.putInt(position
					        + (Long.SIZE / 8 + (i + 1) * Integer.SIZE / 8),
					        currentValue + (int) count);
				}

				buf.putLong(currentPos + i * Long.SIZE / 8,
				        buf.getLong(currentPos + i * Long.SIZE / 8)
				                + finalValue);
			}

		}
		// log.info("stop agg pos:"+buf.position());
	}

	@Override
	public Object get(ByteBuffer buf, int position) {
		// log.info("start get pos:"+buf.position());
		// log.info(" get pos:"+position);
		FrequencyCap freCap = new FrequencyCap();
		// ByteBuffer mutationBuffer = buf.duplicate();
		// mutationBuffer.position(position);
		int currentPosition = position + Long.SIZE / 8
		        + (FrequencyCapAggregatorFactory.BUCKET_SIZE + 1)
		        * Integer.SIZE / 8;
		// if (!type.equals("none")) {
		// currentPosition = position + Long.SIZE / 8 + 2 * Integer.SIZE / 8;
		// }
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			TIntByteHashMap ret = new TIntByteHashMap();

			for (int j = 0; j < HllAggregator.m; j++) {

				if (buf.get(currentPosition + j) != 0) {
					if (i == FrequencyCapAggregatorFactory.BUCKET_SIZE - 1) {
						// log.info("pos value:" + buf.get(currentPosition +
						// j)+"pos:"+(currentPosition + j));
					}
					ret.put(j, buf.get(currentPosition + j));
				}
			}
			freCap.setHll(i, ret);

			currentPosition += HllAggregator.m;
		}
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			// log.info("cout pos value:" + buf.getLong(currentPosition + i *
			// Long.SIZE / 8)+"pos:"+(currentPosition + i * Long.SIZE / 8));
			freCap.setCount(i, buf.getLong(currentPosition + i * Long.SIZE / 8));
		}
		// log.info("agg get:" +
		// freCap.getBucketHLL(FrequencyCapAggregatorFactory.BUCKET_SIZE-1));
		// log.info("stop get pos:"+buf.position());
		return freCap;
	}

	@Override
	public float getFloat(ByteBuffer buf, int position) {
		throw new UnsupportedOperationException(
		        "FrequencyCapBufferAggregator does not support getFloat()");
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
