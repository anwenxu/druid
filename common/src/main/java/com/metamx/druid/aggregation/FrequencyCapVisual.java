package com.metamx.druid.aggregation;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class FrequencyCapVisual {
	  @JsonProperty final public long[] userCount;
	  @JsonProperty
	  final public long[] impressionCount;
	  // an array of the quantiles including the min. and max.

	  @JsonCreator
	  public FrequencyCapVisual(
	      @JsonProperty long[] userCount,
	      @JsonProperty long[] impressionCount
	  )
	  {

	    this.userCount = userCount;
	    this.impressionCount = impressionCount;
	  }

//	  public FrequencyCapVisual(
//	        float[] breaks,
//	        float[] counts,
//	        float[] quantiles
//	  )
//	  {
//	    Preconditions.checkArgument(breaks != null, "breaks must not be null");
//	    Preconditions.checkArgument(counts != null, "counts must not be null");
//	    Preconditions.checkArgument(breaks.length == counts.length + 1, "breaks.length must be counts.length + 1");
//
//	    this.breaks = new double[breaks.length];
//	    this.counts = new double[counts.length];
//	    this.quantiles = new double[quantiles.length];
//	    for(int i = 0; i < breaks.length; ++i) this.breaks[i] = breaks[i];
//	    for(int i = 0; i < counts.length; ++i) this.counts[i] = counts[i];
//	    for(int i = 0; i < quantiles.length; ++i) this.quantiles[i] = quantiles[i];
//	  }

	  @Override
	  public String toString()
	  {
	    return "FrequencyVisual{" +
	           "userCount=" + Arrays.toString(impressionCount) +
	           ", impressionCount=" + Arrays.toString(impressionCount) +
	           '}';
	  }
}
