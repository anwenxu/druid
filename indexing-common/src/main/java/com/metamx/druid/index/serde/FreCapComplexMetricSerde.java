package com.metamx.druid.index.serde;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntShortHashMap;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

import com.metamx.common.logger.Logger;
import com.metamx.druid.aggregation.FrequencyCap;
import com.metamx.druid.index.column.ColumnBuilder;
import com.metamx.druid.index.column.ValueType;
import com.metamx.druid.index.v1.serde.ComplexMetricExtractor;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.index.v1.serde.ComplexMetrics;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.ObjectStrategy;

public class FreCapComplexMetricSerde extends ComplexMetricSerde {
	private static final Logger log = new Logger(FreCapComplexMetricSerde.class);
	public static void registerFreCapSerde() {
		if (ComplexMetrics.getSerdeForType("frecap") == null) {
			ComplexMetrics.registerSerde("frecap",
			        new FreCapComplexMetricSerde());
		}
	}

	@Override
	public String getTypeName() {
		// TODO Auto-generated method stub
		return "frecap";
	}

	@Override
	public ComplexMetricExtractor getExtractor() {
		// TODO Auto-generated method stub
		return new FreCapComplexMetricExtractor();
	}

	@Override
	public ColumnPartSerde deserializeColumn(ByteBuffer buffer,
	        ColumnBuilder builder) {
		GenericIndexed column = GenericIndexed
		        .read(buffer, getObjectStrategy());
		builder.setType(ValueType.COMPLEX);
		builder.setComplexColumn(new ComplexColumnPartSupplier("frecap", column));
		return new ComplexColumnPartSerde(column, "freCcap");
	}

	@Override
	public ObjectStrategy getObjectStrategy() {
		return new FreCapObjectStrategy<FrequencyCap>();
	}

	public static class FreCapObjectStrategy<T> implements ObjectStrategy<T> {
		@Override
		public Class<? extends T> getClazz() {
			return (Class<? extends T>) String.class;
		}

		@Override
		public T fromByteBuffer(ByteBuffer buffer, int numBytes) {
			
	        return (T) new FrequencyCap(buffer);
		}

		@Override
		public byte[] toBytes(T val) {
			FrequencyCap fCap = (FrequencyCap) val;
		    return fCap.toByte();
		}

		@Override
		public int compare(T o1, T o2) {
			 if ((o1).equals(o2)) {
			        return 0;
			      } else {
			        return 1;
			      }
		}

		@Override
		public boolean equals(Object obj) {
			return this.equals(obj);
		}

	}

	public static class FreCapComplexMetricExtractor implements
	        ComplexMetricExtractor {

		@Override
		public Class<?> extractedClass() {
			return List.class;
		}

		@Override
		public Object extractValue(InputRow inputRow, String metricName) {
			return inputRow.getRawData(metricName);
		}

	}


}
