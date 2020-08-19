/**
 * Copyright  Vitalii Rudenskyi (vrudenskyi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mckesson.kafka.connect.sftp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(SftpSinkTask.class);

  private long pollInterval;

  private Map<TopicPartition, Long> pausedPartitions = new HashMap<>();

  private SftpWriter sftpWriter;

  @Override
  public String version() {
    return SftpSinkConnector.version;
  }

  @Override
  public void start(Map<String, String> props) {

    SftpSinkConfig conf = new SftpSinkConfig(props);
    this.pollInterval = conf.getLong(SftpSinkConfig.POLL_INTERVAL_CONFIG);
    this.sftpWriter = new SftpWriter();
    this.sftpWriter.configure(props);
    log.debug("Writer started for {}", context.assignment());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records == null || records.size() == 0) {
      return;
    }
    log.debug("Put for {} records.", records.size());
    checkPaused();
    long startTime = System.currentTimeMillis();
    //data will be written async just enqueue
    Map<TopicPartition, Long> times = sftpWriter.enqueueRecords(records);

    for (Map.Entry<TopicPartition, Long> e : times.entrySet()) {
      long age = System.currentTimeMillis() - e.getValue();
      if (pollInterval > 0 && age < pollInterval) {
        log.debug("Put {} on pause for {}", e.getKey(), pollInterval);
        context.pause(e.getKey());
        pausedPartitions.put(e.getKey(), System.currentTimeMillis() + pollInterval);
      }
    }
    log.debug("Put complete in {} millis", System.currentTimeMillis() - startTime);
  }

  private void checkPaused() {
    if (pollInterval > 0 && pausedPartitions.size() > 0) {
      final List<TopicPartition> resumedPartitions = new ArrayList<>(pausedPartitions.size());
      pausedPartitions.forEach((tp, time) -> {
        if (System.currentTimeMillis() > time) {
          resumedPartitions.add(tp);
        }
      });
      log.debug("Resume partitions: {} ", resumedPartitions);
      resumedPartitions.forEach(p -> {
        context.resume(p);
        pausedPartitions.remove(p);
      });
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    log.debug("Explicit flush for: {}", currentOffsets.keySet());
    currentOffsets.keySet().forEach(tp -> {
      sftpWriter.flush(tp);
    });
    log.debug("Explicit flush done for: {}", currentOffsets.keySet());
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    sftpWriter.close(partitions);
    if (partitions.size() > 0) {
      for (TopicPartition tp : partitions) {
        pausedPartitions.remove(tp);
      }
    }

  }

  @Override
  public void stop() {
    try {
      sftpWriter.close();
    } catch (IOException e) {
      log.error("Error while closing writer");
    }
  }

}
