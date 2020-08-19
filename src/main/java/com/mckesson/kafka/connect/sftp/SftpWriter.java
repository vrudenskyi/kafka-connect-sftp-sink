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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.connect.utils.ELUtils;
import com.mckesson.kafka.connect.utils.SslUtils;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.RemoteFile.RemoteFileOutputStream;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.FingerprintVerifier;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyPairWrapper;

public class SftpWriter implements Configurable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(SftpWriter.class);

  private final Map<TopicPartition, Map<SftpFile, LinkedList<SinkRecord>>> partitions;

  private String host;
  private Integer port;
  private String user;
  private Password passwd;
  private KeyPair keyPair;
  private String fingerprint;

  private String pathTemplate;
  private String fileNameTemplate;
  private String dataOutputTemplate;
  private Boolean dataOutputTemplateEnabled;
  private String outputDelimeter;

  private String tmpPath, tmpFile;
  private long tmpFileMaxInactive;
  private long tmpFileMaxSize;

  private Integer flushCacheSize;
  private Integer writeChunkSize;

  private SSHClient sshClient;
  private SFTPClient sftpClient;

  private long sleepOnFail;
  private int retriesOnFail;

  public SftpWriter() {
    //the reason why we need 'Conncurrent' collection: stop and enqueue may be called from different threads on task close.
    //TODO fix with create 100% threadsafe impl
    this.partitions = new ConcurrentHashMap<>();

  }

  @Override
  public void configure(Map<String, ?> configs) {
    SftpSinkConfig conf = new SftpSinkConfig(configs);
    host = conf.getString(SftpSinkConfig.HOST_CONFIG);
    port = conf.getInt(SftpSinkConfig.PORT_CONFIG);
    user = conf.getString(SftpSinkConfig.USER_CONFIG);
    passwd = conf.getPassword(SftpSinkConfig.PASSWORD_CONFIG);
    fingerprint = conf.getString(SftpSinkConfig.FINGERPRINT_CONFIG);
    keyPair = getKeyPair(conf);

    pathTemplate = conf.getString(SftpSinkConfig.PATH_TEMPLATE_CONFIG);
    if (!pathTemplate.endsWith("/")) {
      pathTemplate = pathTemplate + "/";
    }
    fileNameTemplate = conf.getString(SftpSinkConfig.FILE_TEMPLATE_CONFIG);

    flushCacheSize = conf.getInt(SftpSinkConfig.WRITER_FLUSH_CACHE_SIZE_CONFIG);
    writeChunkSize = conf.getInt(SftpSinkConfig.WRITER_WRITE_CHUNK_SIZE_CONFIG);
    retriesOnFail = conf.getInt(SftpSinkConfig.WRITER_RETRIES_ON_FAIL_CONFIG);
    sleepOnFail = conf.getLong(SftpSinkConfig.WRITER_SLEEP_ON_FAIL_CONFIG);

    dataOutputTemplate = StringEscapeUtils.unescapeJava(conf.getString(SftpSinkConfig.WRITER_RECORD_OUTPUT_TEMPLATE_VALUE_CONFIG));
    dataOutputTemplateEnabled = conf.getBoolean(SftpSinkConfig.WRITER_RECORD_OUTPUT_TEMPLATE_ENABLED_CONFIG);
    outputDelimeter = StringEscapeUtils.unescapeJava(conf.getString(SftpSinkConfig.WRITER_RECORD_OUTPUT_DELIMETER_CONFIG));

    tmpPath = conf.getString(SftpSinkConfig.TMPPATH_TEMPLATE_CONFIG);
    tmpFile = conf.getString(SftpSinkConfig.TMPFILE_TEMPLATE_CONFIG);
    tmpFileMaxInactive = conf.getLong(SftpSinkConfig.TMPFILE_MAX_INACTIVE_TIME_CONFIG);
    tmpFileMaxSize = conf.getLong(SftpSinkConfig.TMPFILE_MAX_SIZE_CONFIG);
  }

  /**
   * Enqueue records 
   * 
   * @param records
   * @return -  max timestamps per partition
   * @throws ConnectException
   */
  public Map<TopicPartition, Long> enqueueRecords(Collection<SinkRecord> records) throws ConnectException {

    Map<TopicPartition, Long> maxTimestamps = new HashMap<>();
    Set<TopicPartition> bucketPartitions = new HashSet<>();
    for (SinkRecord r : records) {
      TopicPartition tp = new TopicPartition(r.topic(), r.kafkaPartition());
      if (partitions.putIfAbsent(tp, new ConcurrentHashMap<>()) == null) {
        log.warn("Record for not opened TopicPartition: {}", tp);
      }
      bucketPartitions.add(tp);

      SftpFile file = new SftpFile(ELUtils.getExprValue(pathTemplate, r), ELUtils.getExprValue(fileNameTemplate, r), ELUtils.getExprValue(tmpPath, r), ELUtils.getExprValue(tmpFile, r));
      partitions.get(tp).putIfAbsent(file, new LinkedList<>());
      partitions.get(tp).get(file).add(r);
      long rts = r.timestampType().equals(TimestampType.NO_TIMESTAMP_TYPE) ? 0L : r.timestamp();

      if (maxTimestamps.getOrDefault(tp, Long.valueOf(0L)) < rts) {
        maxTimestamps.put(tp, rts);
      }
    }

    for (TopicPartition tp : bucketPartitions) {
      final MutableInt partitionSize = new MutableInt(0);
      partitions.get(tp).values().forEach(e -> {
        partitionSize.add(e.size());
      });

      if (partitionSize.intValue() > 0) {
        log.debug("     {}: size={}, age={}", tp, partitionSize.intValue(), (System.currentTimeMillis() - maxTimestamps.getOrDefault(tp, Long.valueOf(0L))));
        if (partitionSize.intValue() > flushCacheSize) {
          log.debug("    {}: cache size exeeded. flush.", tp);
          flush(tp);
        }
      }

    }
    return maxTimestamps;
  }

  public void close(TopicPartition tp) {

    log.debug("Close TopicPartition: {}", tp);
    flush(tp);

    log.debug("Rename files on partition close: {}", tp);
    Map<SftpFile, LinkedList<SinkRecord>> tpEntry = partitions.remove(tp);
    if (tpEntry == null) {
      return;
    }
    for (SftpFile sftpFile : tpEntry.keySet()) {
      try {
        renameSftpFile(sftpFile);
        tpEntry.remove(sftpFile);
      } catch (Exception e) {
        throw new ConnectException("Failed to rename file on partition close", e);
      }
    }
  }

  public void close(Collection<TopicPartition> partitions) {
    log.debug("Close partitions from cache: {}", partitions);
    for (TopicPartition tp : partitions) {
      close(tp);
    }
  }

  private SFTPClient getSftpClient() throws IOException {
    if (this.sftpClient == null) {
      this.sftpClient = getSshClient().newSFTPClient();
    }
    return this.sftpClient;
  }

  private SSHClient getSshClient() throws IOException {
    if (this.sshClient == null) {
      this.sshClient = new SSHClient();
      HostKeyVerifier hostVerifier = StringUtils.isNotBlank(fingerprint) ? FingerprintVerifier.getInstance(fingerprint) : new PromiscuousVerifier();
      this.sshClient.addHostKeyVerifier(hostVerifier);
      this.sshClient.connect(host, port);
      if (user != null && passwd != null) {
        this.sshClient.authPassword(user, passwd.value());
      } else if (user != null && keyPair != null) {
        this.sshClient.authPublickey(user, new KeyPairWrapper(keyPair));
      }
    }
    return this.sshClient;

  }

  public Integer flush(TopicPartition tp) {

    Integer recordsFlushed = 0;
    Map<SftpFile, LinkedList<SinkRecord>> tpEntry = partitions.get(tp);
    if (tpEntry == null || tpEntry.size() == 0) {
      return 0;
    }
    for (Map.Entry<SftpFile, LinkedList<SinkRecord>> fileEntry : tpEntry.entrySet()) {
      int retriesCount = retriesOnFail;
      boolean done = false;
      while (!done) {
        try {
          SftpFile sftpFile = fileEntry.getKey();
          LinkedList<SinkRecord> recs = fileEntry.getValue();
          long fileLenght = 0L;
          if (recs.size() > 0) {
            log.debug("Writing {} records to tmp file {}", recs.size(), sftpFile.getFullTmpPath());
            sftpFile.setLastWriteTime(System.currentTimeMillis());
            if (!sftpFile.isPathCreated()) {
              if (getSftpClient().statExistence(sftpFile.getPath()) == null) {
                log.debug("Create path {}", sftpFile.getPath());
                getSftpClient().mkdirs(sftpFile.getPath());
              }
              sftpFile.setPathCreated(true);
            }
            if (sftpFile.isUseTmp() && !sftpFile.isTmpPathCreated()) {
              if (getSftpClient().statExistence(sftpFile.getTmpPath()) == null) {
                log.debug("Create tmp path {}", sftpFile.getTmpPath());
                getSftpClient().mkdirs(sftpFile.getTmpPath());
              }
              sftpFile.setTmpPathCreated(true);
            }

            try (
                RemoteFile rFile = getSftpClient().open(sftpFile.getFullTmpPath(), EnumSet.of(OpenMode.CREAT, OpenMode.APPEND, OpenMode.WRITE));
                RemoteFileOutputStream rFileOut = rFile.new RemoteFileOutputStream(rFile.length());) {

              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              long totalSize = 0;
              for (SinkRecord r : recs) {
                if (dataOutputTemplateEnabled) {
                  baos.write(ELUtils.getExprValue(dataOutputTemplate, r).getBytes());
                } else {
                  baos.write(r.value() == null ? "".getBytes() : r.value().toString().getBytes());
                }
                baos.write(outputDelimeter.getBytes());
                if (baos.size() >= writeChunkSize) {
                  totalSize += baos.size();
                  baos.writeTo(rFileOut);
                  rFileOut.flush();
                  baos.reset();
                }

              }
              totalSize += baos.size();
              baos.writeTo(rFileOut);
              rFileOut.flush();
              recordsFlushed += recs.size();
              fileLenght = rFile.length();
              log.debug("Done writing {} recs, size : {}. Saved to file: {}", recs.size(), totalSize, sftpFile.getFullTmpPath());
              recs.clear();
            }

          }

          if ((sftpFile.getLastWriteTime() != 0L && (System.currentTimeMillis() - sftpFile.getLastWriteTime()) > tmpFileMaxInactive) || //time based 
              (tmpFileMaxSize > 0 && fileLenght > tmpFileMaxSize) //size based
          ) {
            log.debug("File {} was not touched since {} or size {} > {}. Remove tpEntry cache.", sftpFile.getFullTmpPath(), new Date(sftpFile.getLastWriteTime()), fileLenght, tmpFileMaxSize);
            renameSftpFile(sftpFile);
            tpEntry.remove(sftpFile);
          }

          done = true;
        } catch (Exception e) {
          log.error("Failed to flush data for partition:{},  sftpFile: {}. To be retried {} times after sleep", tp, fileEntry.getKey().getFullTmpPath(), retriesCount, e);
          if (retriesCount > 0) {
            retriesCount--;
            sleep(sleepOnFail);
          } else {
            throw new ConnectException("flushTp failed after retries:  " + tp, e);
          }
        }
      }
    }
    return recordsFlushed;
  }

  private void renameSftpFile(SftpFile sftpFile) {
    if (!sftpFile.isUseTmp()) {
      return;
    }
    log.debug("Renaming sftp file from {} to {}", sftpFile.getFullTmpPath(), sftpFile.getFullPath());
    try {
      SFTPClient sftpClient = getSftpClient();

      if (sftpClient.statExistence(sftpFile.getFullTmpPath()) == null) {
        log.warn("Temporary file {} does not exist. Consider to use ${partition} in tmp file name.", sftpFile.getFullTmpPath());
        return;
      }

      FileAttributes attrs = sftpClient.statExistence(sftpFile.getFullPath());
      if (attrs != null) {
        String oldName = sftpFile.getFullPath();
        String suffix = "-" + Long.toString(System.currentTimeMillis());
        int idx = oldName.lastIndexOf('.');
        String newName;
        if (idx == -1) {
          newName = oldName + suffix;
        } else {
          newName = oldName.substring(0, idx) + suffix + oldName.substring(idx);
        }
        log.warn("Final file {} already exists. Will be saved as: {}", oldName, newName);
        sftpClient.rename(sftpFile.getFullTmpPath(), newName);
      } else {
        sftpClient.rename(sftpFile.getFullTmpPath(), sftpFile.getFullPath());
      }
    } catch (IOException e) {
      log.warn("Rename failed. Check for temporary files", e);
    }

  }

  @Override
  public void close() throws IOException {
    log.debug("Writer stopped. Close all partitions");
    partitions.keySet().forEach(tp -> {
      close(tp);
    });
    log.debug("All closed. Disconnect.");
    disconnect();

  }

  public void disconnect() throws IOException {
    log.debug("Close SFTP connection");
    if (sftpClient != null) {
      sftpClient.close();
      sftpClient = null;
    }
    if (sshClient != null) {
      sshClient.close();
      sshClient = null;
    }
    log.debug("Writer closed");

  }

  private KeyPair getKeyPair(SftpSinkConfig config) throws ConnectException {

    final String authKeyAlias = config.getString(SftpSinkConfig.AUTH_KEY_ALIAS_CONFIG);
    if (StringUtils.isBlank(authKeyAlias)) {
      return null;
    }

    try {
      KeyStore keyStore = SslUtils.loadKeyStore(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
          config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
          config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));

      Key key = keyStore.getKey(authKeyAlias, config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG).value().toCharArray());
      if (key instanceof PrivateKey) {
        // Get certificate of public key
        Certificate cert = keyStore.getCertificate(authKeyAlias);

        // Get public key
        PublicKey publicKey = cert.getPublicKey();

        // Return a key pair
        return new KeyPair(publicKey, (PrivateKey) key);
      }
    } catch (Exception e) {
      throw new ConnectException("Failed to load key", e);
    }

    throw new ConnectException("Was not able to load keypair from: " + authKeyAlias);
  }

  private void sleep(long millis) {
    sleep(millis, 100L);
  }

  private void sleep(long millis, long checkInterval) {
    long wakeuptime = System.currentTimeMillis() + millis;
    while (true) {
      try {
        Thread.sleep(checkInterval);
      } catch (InterruptedException e) {
        log.debug("Sleep interrupted");
      }
      if (System.currentTimeMillis() >= wakeuptime || Thread.currentThread().isInterrupted())
        break;
    }
  }

}
