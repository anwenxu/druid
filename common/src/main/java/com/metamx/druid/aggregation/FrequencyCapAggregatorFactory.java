package com.metamx.druid.aggregation;

import gnu.trove.map.TIntByteMap;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntShortHashMap;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ColumnSelectorFactory;

public class FrequencyCapAggregatorFactory implements AggregatorFactory {
	private final String name;
	private final String uid = "user_id";
	private final String count="impCount";
	private final String capType;
	private final short frequency;
	private final String bucketId = "bucket_num_id";
	private final String hll = "userCount";
	private static final Logger log = new Logger(
	        FrequencyCapAggregatorFactory.class);

	public static final int BUCKET_SIZE = 37;

	@JsonCreator
	public FrequencyCapAggregatorFactory(@JsonProperty("name") String name,
	        @JsonProperty("capType") final String capType, // "day","week","month",
	                                                       // "multidays"
	        @JsonProperty("frequency") final short frequency) {
		// log.info("name:"+name+"   uid:"+uid+"   capType:"+capType+"     frequency:"+frequency+"     count:"+count);
		Preconditions.checkNotNull(name,
		        "Must have a valid, non-null aggregator name");
		Preconditions.checkNotNull(uid, "Must have a valid, non-null uid");
		Preconditions.checkNotNull(capType,
		        "Must have a valid, non-null capType");
		Preconditions.checkNotNull(frequency,
		        "Must have a valid, non-null frequency");
		Preconditions.checkNotNull(count, "Must have a valid, non-null count");

		this.name = name;
		this.capType = capType;
		this.frequency = frequency;
	}

	@Override
	public Aggregator factorize(ColumnSelectorFactory metricFactory) {
		if(metricFactory.makeComplexMetricSelector(name)!=null)
		return new FrequencyCapAggregator(name,
		        metricFactory.makeComplexMetricSelector(name));
		else
			return new FrequencyCapAggregator(name,
					metricFactory.makeObjectColumnSelector(uid),
			        metricFactory.makeTimestampColumnSelector(),
			        metricFactory.makeObjectColumnSelector(bucketId),
			        metricFactory.makeObjectColumnSelector(count),
			        metricFactory.makeObjectColumnSelector(hll), capType, frequency);	
	}

	static final Comparator COMPARATOR = new Comparator()
	  {
	    @Override
	    public int compare(Object o, Object o1)
	    {
	    	for(int i = 0;i<BUCKET_SIZE; i++){
	    		if(!((FrequencyCap) o).getBucketHLL(i).equals( ((FrequencyCap) o1).getBucketHLL(i))){
	    			return -1;
	    		}
	    		if(!(((FrequencyCap) o).getBucketCount(i) == ( ((FrequencyCap) o1).getBucketCount(i)))){
	    			return -1;
	    		}
	    	}
	    	return 0;
	    }
	  };

	@Override
	public BufferAggregator factorizeBuffered(
	        ColumnSelectorFactory metricFactory) {
		// TODO Auto-generated method stub
		return new FrequencyCapBufferAggregator(
		        metricFactory.makeObjectColumnSelector(uid),
		        metricFactory.makeTimestampColumnSelector(),
		        metricFactory.makeObjectColumnSelector(bucketId),
		        metricFactory.makeObjectColumnSelector(count),
		        metricFactory.makeObjectColumnSelector(hll), capType, frequency);
	}

	@Override
	public Comparator getComparator() {
		return COMPARATOR;
	}

	Object combineFreCap(Object lhs, Object rhs) {
		FrequencyCap frequencyCap = new FrequencyCap();
		frequencyCap.combine((FrequencyCap) lhs);
		frequencyCap.combine((FrequencyCap) rhs);
		
		return frequencyCap;
	}

	@Override
	public Object combine(Object lhs, Object rhs) {
		// log.info("combine cap combining "+lhs+rhs);
		return combineFreCap(lhs, rhs);
	}

	@Override
	public AggregatorFactory getCombiningFactory() {
		// log.info("factory name:"+name);
		return new FrequencyCapAggregatorFactory(name, capType,
		        frequency);
	}

	@Override
	public Object deserialize(Object object) {
//		 log.info("class name:"+object.getClass()+":value "+object);

		String k = (String) object;
		byte[] ibmapByte = Base64.decodeBase64(k);

		ByteBuffer buffer = ByteBuffer.wrap(ibmapByte);
		
		FrequencyCap cap = new FrequencyCap(buffer);
//		log.info("des"+cap.toString());
		return cap;
	}

	@Override
	public Object finalizeComputation(Object object) {
		
		FrequencyCap frequencyCap = (FrequencyCap) object;
//		log.info("final"+frequencyCap.toString());
		return frequencyCap.asVisual();
	}

	@Override
	@JsonProperty
	public String getName() {
		return name;
	}

	@JsonProperty
	public String getUid() {
		return uid;
	}

	@JsonProperty
	public String getCount() {
		return count;
	}

	@JsonProperty
	public String getCapType() {
		return capType;
	}

	@JsonProperty
	public short getFrequency() {
		return frequency;
	}

	@Override
	public List<String> requiredFields() {
		String[] fields = { uid, count, hll };
		return Arrays.asList(fields);
	}

	@Override
	public byte[] getCacheKey() {
		byte[] fieldNameBytes = (name + capType + frequency)
		        .getBytes();
		return ByteBuffer.allocate(1 + fieldNameBytes.length).put((byte) 0x37)
		        .put(fieldNameBytes).array();
	}

	@Override
	public String getTypeName() {
		return "frecap";
	}

	@Override
	public int getMaxIntermediateSize() {
		return Long.SIZE / 8 + (FrequencyCapAggregatorFactory.BUCKET_SIZE+1) * Integer.SIZE / 8+FrequencyCapAggregatorFactory.BUCKET_SIZE
		        * HllAggregator.m+ Long.SIZE / 8*FrequencyCapAggregatorFactory.BUCKET_SIZE;
	}

	@Override
	public Object getAggregatorStartValue() {
		return new FrequencyCap();
	}

}
