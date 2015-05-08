/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.firehose.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONObject;

import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.firehose.kafka.KafkaSimpleConsumer.BytesMessageWithOffset;

public class KafkaEightSimpleConsumerFirehoseFactory implements
        FirehoseFactoryV2<ByteBufferInputRowParser> {
	private static final Logger log = new Logger(
	        KafkaEightSimpleConsumerFirehoseFactory.class);

	@JsonProperty
	private final String brokerList;

	@JsonProperty
	private final String partitionIdList;

	@JsonProperty
	private final String clientId;

	@JsonProperty
	private final String feed;

	@JsonProperty
	private final String offsetPosition;

	@JsonProperty
	private final int queueBufferLength;

	private final Map<Integer, KafkaSimpleConsumer> simpleConsumerMap = new HashMap<Integer, KafkaSimpleConsumer>();

	private final static int fileRetention = 5;

	private final Map<Integer, Long> lastOffsetPartitions = new HashMap<Integer, Long>();

	private static final String PARTITION_SEPERATOR = " ";
	private static final String PARTITION_OFFSET_SEPERATOR = ",";
	private static final String FILE_NAME_SEPERATOR = "_";

	/*private MessageDigest md;*/

/*	private FixedFileArrayList<File> offsetFileList = new FixedFileArrayList<File>(
	        fileRetention);
	private FixedFileArrayList<Object> offsetObjectList = new FixedFileArrayList<Object>(
      fileRetention);
*/
	private List<Thread> consumerThreadList = new ArrayList<Thread>();
	
	private  InputRow currMsg;
	private BytesMessageWithOffset msg;
	private boolean stop;

	@JsonCreator
	public KafkaEightSimpleConsumerFirehoseFactory(
	        @JsonProperty("brokerList") String brokerList,
	        @JsonProperty("partitionIdList") String partitionIdList,
	        @JsonProperty("clientId") String clientId,
	        @JsonProperty("feed") String feed,
	        @JsonProperty("offsetPosition") String offsetPosition,
	        @JsonProperty("queueBufferLength") int queueBufferLength) {
		this.brokerList = brokerList;
		this.partitionIdList = partitionIdList;
		this.clientId = clientId;
		this.feed = feed;
		this.queueBufferLength = queueBufferLength;
		this.offsetPosition = offsetPosition;
	}
	private void loadOffsetFromPreviousMetaData(Object lastCommit) {
		if (lastCommit == null) {
			return;
		}
		try {
			JSONObject lastCommitObject = new JSONObject(lastCommit.toString());
			Iterator<String> keysItr = lastCommitObject.keys();
			while(keysItr.hasNext()) {
				String key = keysItr.next();
	         int partitionId = Integer.parseInt(key);
	         Long offset = lastCommitObject.getLong(key);
	         log.debug("Recover last commit information partitionId [%s], offset [%s]", partitionId, offset);
	         if (lastOffsetPartitions.containsKey(partitionId)) {
			        if(lastOffsetPartitions.get(partitionId) < offset) {
			        	lastOffsetPartitions.put(partitionId, offset);
			        }
					} else {
						lastOffsetPartitions.put(partitionId, offset);
					}
	    
	     }
		} catch (Exception e) {
			log.error("Fail to load offset from previous meta data [%s]", lastCommit);
		}

		log.info("Loaded offset map: " + lastOffsetPartitions);

	}

	@Override
	public FirehoseV2 connect(final ByteBufferInputRowParser firehoseParser, Object lastCommit) throws IOException 
	{
		Set<String> newDimExclus = Sets.union(
				firehoseParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
		        Sets.newHashSet("feed")
				);
		final ByteBufferInputRowParser theParser = firehoseParser.withParseSpec(
				firehoseParser.getParseSpec()
		                .withDimensionsSpec(
		                        firehoseParser.getParseSpec()
		                                .getDimensionsSpec()
		                                .withDimensionExclusions(
		                                		newDimExclus
		                                		)
		                		)
				);
		final LinkedBlockingQueue<BytesMessageWithOffset> messageQueue = new LinkedBlockingQueue(
		        queueBufferLength);
		class partitionConsumerThread extends Thread {
			private int partitionId;
			private boolean stop = false;

			partitionConsumerThread(int partitionId) {
				this.partitionId = partitionId;
			}

			@Override
			public void run() {
				log.info("Start running parition [%s] thread name : [%s]",partitionId, getName());
				try {
					Long offset = lastOffsetPartitions.get(partitionId);
					if (offset == null) {
						offset = 0L;
					}
					while (!isInterrupted()) {
						try {
							Iterable<BytesMessageWithOffset> msgs = simpleConsumerMap
							        .get(partitionId).fetch(offset, 10000);
							int count = 0;
							for (BytesMessageWithOffset msgWithOffset : msgs) {
								offset = msgWithOffset.offset();
								messageQueue.put(msgWithOffset);
								count++;
							}
						} catch (InterruptedException e) {
							log.error("Intrerupted when fecthing data");
							return;
						}
					}
				} finally {
					simpleConsumerMap.get(partitionId).stop();
				}
			}

		}
		;

		//loadOffsetFromDisk();
		loadOffsetFromPreviousMetaData(lastCommit);
		log.info("Kicking off all consumer");
		final Iterator<BytesMessageWithOffset> iter = messageQueue.iterator();

		for (String partitionStr : Arrays.asList(partitionIdList.split(","))) {
			int partition = Integer.parseInt(partitionStr);
			final KafkaSimpleConsumer kafkaSimpleConsumer = new KafkaSimpleConsumer(
			        feed, partition, clientId, Arrays.asList(brokerList
			                .split(",")));
			simpleConsumerMap.put(partition, kafkaSimpleConsumer);
			Thread t = new partitionConsumerThread(partition);
			consumerThreadList.add(t);
			t.start();
		}
		log.info("All consumer started");
		return new FirehoseV2() {
			@Override
			public void start() throws Exception {
				//TODO
			}
			@Override
			public boolean advance() { 
				if(stop){
					return false;
				}
				lastOffsetPartitions.put(msg.getPartition(), msg.offset());
				return true;
			}

			@Override
			public InputRow currRow() {
				try {
					msg = messageQueue.take();
				} catch (InterruptedException e) {
					log.info("Interrupted when taken from queue");
					currMsg = null;
				}
				final byte[] message = msg.message();

				if (message == null) {
					currMsg = null;
				}
				currMsg = theParser.parse(ByteBuffer.wrap(message));				
				return currMsg;
			}

			@Override
			public Committer makeCommitter() {
				/*final java.util.Date date = new java.util.Date();

				StringBuilder fileContent = new StringBuilder();

				for (int partition : lastOffsetPartitions.keySet()) {
					fileContent.append(partition
					        + PARTITION_OFFSET_SEPERATOR
					        + lastOffsetPartitions.get(partition)
					        + PARTITION_SEPERATOR);
				}*/

				Object thisCommit = new JSONObject(lastOffsetPartitions);

				class MyCommitter implements Committer {
					private Object metaData;
					public void setMetaData(Object metaData) {
						this.metaData = metaData;
					}
					@Override
					public Object getMetadata() {
						return metaData;
					}
					@Override
					public void run() {
						// TODO this makes the commit
					}
				};
				MyCommitter committer = new MyCommitter();
				committer.setMetaData(thisCommit);
				return committer;
			}

			@Override
			public void close() throws IOException {
				log.info("Stoping kafka 0.8 simple firehose");
				stop = true;
				for (Thread t : consumerThreadList) {
					try {
						t.interrupt();
						t.join(3000);
					} catch (InterruptedException e) {
						log.info("Interupted when stoping ");
					}
				}
			}
		};
	}
/*
	@Override
	public InputRowParser getParser() {
		// TODO Auto-generated method stub
		return parser;
	}*/
	
}
