package com.metamx.druid.aggregation;

import com.google.common.hash.Hashing;
import com.metamx.druid.processing.ObjectColumnSelector;

import java.nio.ByteBuffer;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.procedure.TIntByteProcedure;

public class HllBufferAggregator implements BufferAggregator {

	private final ObjectColumnSelector selector;

	public HllBufferAggregator(ObjectColumnSelector selector) {
		this.selector = selector;
	}

	/*
	 * byte 1 key length byte 2 value length byte 3...n key array byte n+1....
	 * value array
	 */
	@Override
	public void init(ByteBuffer buf, int position) {
		for (int i = 0; i < HllAggregator.m; i++) {
			buf.put(position + i, (byte) 0);
		}
	}

	@Override
	public void aggregate(ByteBuffer buf, int position) {
		if (selector.get() instanceof String) {
			final int bucket = HllAggregator.hashInput((String) (selector.get()))[0];
			final int zerolength = HllAggregator.hashInput((String) (selector.get()))[1];
			if (zerolength > buf.get(position + bucket)) {
				buf.put(position + bucket, (byte) zerolength);
			}
		} else {
			final ByteBuffer fb = buf;
			final int fp = position;
			TIntByteHashMap newobj = (TIntByteHashMap) (selector.get());
			newobj.forEachEntry(new TIntByteProcedure() {
				public boolean execute(int a, byte b) {
					if (b > fb.get(fp + a)) {
						fb.put(fp + a, b);
					}
					return true;
				}
			});
		}
	}

	@Override
	public Object get(ByteBuffer buf, int position) {
		TIntByteHashMap ret = new TIntByteHashMap();
		for (int i = 0; i < HllAggregator.m; i++) {
			if (buf.get(position + i) != 0) {
				ret.put(i, buf.get(position + i));
			}
		}
		return ret;
	}

	@Override
	public float getFloat(ByteBuffer buf, int position) {
		throw new UnsupportedOperationException(
				"HllAggregator does not support getFloat()");
	}

	@Override
	public void close() {
	}
}
