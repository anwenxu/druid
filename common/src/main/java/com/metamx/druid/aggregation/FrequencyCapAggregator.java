package com.metamx.druid.aggregation;

import java.util.Calendar;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntShortHashMap;

import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ComplexMetricSelector;
import com.metamx.druid.processing.ObjectColumnSelector;
import com.metamx.druid.processing.TimestampColumnSelector;

public class FrequencyCapAggregator implements Aggregator {
	private final String name;
	private static final Logger log = new Logger(FrequencyCapAggregator.class);
	private final ComplexMetricSelector selector;
	
	private final ObjectColumnSelector uidSelector;
	private final TimestampColumnSelector timestampSelector;
	private final ObjectColumnSelector countSelector;
	private final ObjectColumnSelector bucketIdSelector;
	private final ObjectColumnSelector uuhllSelector;
	private final String type;
	private final short fre;
	private FrequencyCap fCap;
	private long lastUid= 0;
	private int lastTimeKey = 0;
	private int currentValue = 0;

	public FrequencyCapAggregator(String name,
	        ComplexMetricSelector complexMetricSelector) {
		this.name = name;
		fCap = new FrequencyCap();
		selector = complexMetricSelector;
		this.uidSelector = null;
		this.timestampSelector = null;
		this.countSelector =null;
		this.type = null;
		this.fre = 0;
		this.uuhllSelector = null;
		this.bucketIdSelector = null;
	}

	public FrequencyCapAggregator(String name,
			ObjectColumnSelector uidSelector,
	        TimestampColumnSelector timestamp, ObjectColumnSelector bucketId,
	        ObjectColumnSelector count, ObjectColumnSelector uuhll,
	        String type, short fre) {
		this.name = name;
		fCap = new FrequencyCap();
		selector = null;
		this.uidSelector = uidSelector;
		this.timestampSelector = timestamp;
		this.countSelector = count;
		this.type = type;
		this.fre = fre;
		this.uuhllSelector = uuhll;
		this.bucketIdSelector = bucketId;
    }

	@Override
	public void aggregate() {
		
		if (selector!=null && selector.get() instanceof FrequencyCap) {
			Object value = selector.get();
			// log.info("class name:"+value.getClass()+":value "+value);
			FrequencyCap newFCap = (FrequencyCap) value;
			fCap.combine(newFCap);
		}else{
			Long uid = Long.parseLong(uidSelector.get().toString());
			long timestamp = timestampSelector.getTimestamp();
			Float countOrignal = Float.parseFloat(countSelector.get().toString());
			int bucketId = Integer.parseInt(bucketIdSelector.get().toString());
			TIntByteHashMap hllObj = (TIntByteHashMap) (uuhllSelector.get());

			long count = countOrignal.longValue();
			
			for (int i = bucketId; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
				aggHll(fCap, i, hllObj);
			}
			
			
			if (type.equals("none")) {
				for (int i = bucketId; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
					fCap.setCount(i, fCap.getBucketCount(i)+count);
				}
			}else{
				int timeKey = 0;
				Calendar cal = Calendar.getInstance();
				cal.setTimeInMillis((timestamp));
				if (type.equals("day"))
					timeKey = cal.get(Calendar.DAY_OF_YEAR);
				if (type.equals("week"))
					timeKey = cal.get(Calendar.WEEK_OF_YEAR);
				if (type.equals("month"))
					timeKey = cal.get(Calendar.MONTH);
				if (lastUid != uid || lastTimeKey != timeKey) {
					lastUid = uid;
					lastTimeKey = timeKey;
					currentValue = 0;
				}
				
				long finalValue = 0;

				if ((long) currentValue + count > fre) {
					if (fre - currentValue > 0) {
						finalValue = fre - currentValue;
						currentValue = fre;
					}
				} else {
					finalValue = count;
					currentValue += count;
				}
				for (int i = bucketId; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
					fCap.setCount(i, fCap.getBucketCount(i)+finalValue);
				}
			}
		}
		// log.info("after agg"+fCap.toString());
		// log.debug("combineIbMap "+newIbMap);

	}

	private void aggHll(FrequencyCap fCap, int index, TIntByteHashMap hllObj) {
		int[] indexes = hllObj.keys();
		for (int i = 0; i < indexes.length; i++) {
			int index_i = indexes[i];
			if (fCap.getBucketHLL(index).get(index_i) == fCap.getBucketHLL(index).getNoEntryValue()
					|| hllObj.get(index_i) > fCap.getBucketHLL(index).get(index_i)) {
				fCap.getBucketHLL(index).put(index_i, hllObj.get(index_i));
			}
		}
    }

	@Override
	public void reset() {

	}

	@Override
	public Object get() {

		return fCap;
	}

	@Override
	public float getFloat() {
		return 0;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return name;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
