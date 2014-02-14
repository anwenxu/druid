package com.metamx.druid.aggregation;

import gnu.trove.map.hash.TIntByteHashMap;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class FrequencyCap extends Object{
	private TIntByteHashMap[] bucketHLL;
	private long[] impressionCount;
	private long[] hllFinal;

	public FrequencyCap() {
		bucketHLL = new TIntByteHashMap[FrequencyCapAggregatorFactory.BUCKET_SIZE];
		impressionCount = new long[FrequencyCapAggregatorFactory.BUCKET_SIZE];
	}

	public FrequencyCap(ByteBuffer buffer) {
		bucketHLL = new TIntByteHashMap[FrequencyCapAggregatorFactory.BUCKET_SIZE];
		impressionCount = new long[FrequencyCapAggregatorFactory.BUCKET_SIZE];
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			int keylength = buffer.getInt();
			int valuelength = buffer.getInt();
			if (keylength == 0) {
				bucketHLL[i] = new TIntByteHashMap();
			} else {
				int[] keys = new int[keylength];
				byte[] values = new byte[valuelength];

				for (int j = 0; j < keylength; j++) {
					keys[j] = buffer.getInt();
				}
				buffer.get(values);

				bucketHLL[i] = new TIntByteHashMap(keys, values);
			}
		}
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			impressionCount[i] = buffer.getLong();
		}
	}

	public void setHll(int index, TIntByteHashMap tmap) {
		bucketHLL[index] = tmap;
	}

	public void setCount(int index, long count) {
		impressionCount[index] = count;
	}

	public byte[] toByte() {
		ByteBuffer buffer = ByteBuffer.allocate(4 * 1024 * 1024);
		int byteNum = 0;
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			int[] indexesResult = bucketHLL[i].keys();
			byte[] valueResult = bucketHLL[i].values();

			buffer.putInt((int) indexesResult.length);
			buffer.putInt((int) valueResult.length);
			for (int j = 0; j < indexesResult.length; j++) {
				buffer.putInt(indexesResult[j]);
			}
			for (int j = 0; j < valueResult.length; j++) {
				buffer.put(valueResult[j]);
			}
			byteNum += 4 * indexesResult.length + valueResult.length + 8;
		}

		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			buffer.putLong(impressionCount[i]);
			byteNum += 8;
		}
		byte[] result = new byte[byteNum];
		buffer.flip();
		buffer.get(result);
		return result;
	}

	public TIntByteHashMap getBucketHLL(int index) {
		return bucketHLL[index];
	}

	public long getBucketCount(int index) {
		return impressionCount[index];
	}

	public void combine(FrequencyCap cap) {
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			TIntByteHashMap newIbMap = cap.getBucketHLL(i);
			int[] indexes = newIbMap.keys();
			if (bucketHLL[i] == null) {
				bucketHLL[i] = new TIntByteHashMap();
			}
			for (int j = 0; j < indexes.length; j++) {
				int index_j = indexes[j];
				if (bucketHLL[i].get(index_j) == bucketHLL[i].getNoEntryValue()
				        || newIbMap.get(index_j) > bucketHLL[i].get(index_j)) {
					bucketHLL[i].put(index_j, newIbMap.get(index_j));
				}
			}
			impressionCount[i] += cap.getBucketCount(i);
		}
	}

	public FrequencyCapVisual asVisual() {
		calculateHllFinal();
		return new FrequencyCapVisual(hllFinal,impressionCount);
	}

	private void calculateHllFinal() {
		hllFinal = new long[FrequencyCapAggregatorFactory.BUCKET_SIZE];
		for (int i = 0; i < FrequencyCapAggregatorFactory.BUCKET_SIZE; i++) {
			TIntByteHashMap ibMap = bucketHLL[i];
			int[] keys = ibMap.keys();
			double registerSum = 0;
			int count = keys.length;
			double zeros = 0.0;
			for (int j = 0; j < keys.length; j++) {
				{
					int val = ibMap.get(keys[j]);
					registerSum += 1.0 / (1 << val);
					if (val == 0) {
						zeros++;
					}
				}

			}
			registerSum += (HllAggregator.m - count);
			zeros += HllAggregator.m - count;

			double estimate = HllAggregator.alphaMM * (1.0 / registerSum);

			if (estimate <= (5.0 / 2.0) * (HllAggregator.m)) {
				// Small Range Estimate
				hllFinal[i] = Math.round(HllAggregator.m
						* Math.log(HllAggregator.m / zeros));
			} else {
				hllFinal[i] = Math.round(estimate);
			}
		}
	}
	
	@Override
	public String toString(){
		String count="";
		for(int i = 0;i<impressionCount.length;i++){
			count+=" "+impressionCount[i]+",";
		}
		return "bucket hll:"+Arrays.asList(bucketHLL)+"count int:"+count;
		
	}
	
}
