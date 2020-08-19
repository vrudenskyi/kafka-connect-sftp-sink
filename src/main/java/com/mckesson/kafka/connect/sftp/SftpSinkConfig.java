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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class SftpSinkConfig extends AbstractConfig {

  public static final String HOST_CONFIG = "sftp.host";

  public static final String PORT_CONFIG = "sftp.port";
  public static final Integer PORT_DEFAULT = 22;

  public static final String FINGERPRINT_CONFIG = "sftp.fingerpring";

  public static final String USER_CONFIG = "sftp.user";
  public static final String PASSWORD_CONFIG = "sftp.password";

  public static final String AUTH_KEY_ALIAS_CONFIG = "sftp.authKeyAlias";

  public static final String PATH_TEMPLATE_CONFIG = "sftp.pathTemplate";
  public static final String PATH_TEMPLATE_DEFAULT = "${topic}/${timestamp,yyyy}/${timestamp,MM}/${timestamp,dd}";

  public static final String KEEP_CONNECTION_TIME_CONFIG = "sftp.keepConnectionTime";
  public static final Long KEEP_CONNECTION_TIME_DEFAULT = 90000L;

  public static final String FILE_TEMPLATE_CONFIG = "sftp.fileTemplate";
  public static final String FILE_TEMPLATE_DEFAULT = "${partition}.data";

  
  public static final String TMPFILE_MAX_INACTIVE_TIME_CONFIG = "sftp.tmpFile.maxInactiveTime";
  public static final Long TMPFILE_MAX_INACTIVE_TIME_DEFAULT = 10 * 60 * 1000L; //10 mins
  
  
  public static final String TMPFILE_MAX_SIZE_CONFIG = "sftp.tmpFile.maxSize";
  public static final Long TMPFILE_MAX_SIZE_DEFAULT = -1L;
  
  public static final String TMPPATH_TEMPLATE_CONFIG = "sftp.tmpPathTemplate";
  public static final String TMPFILE_TEMPLATE_CONFIG = "sftp.tmpFileTemplate";
  

  public static final String POLL_INTERVAL_CONFIG = "pollInterval";
  public static final Long POLL_INTERVAL_DEFAULT = 0L;

  public static final String WRITER_RECORD_OUTPUT_TEMPLATE_VALUE_CONFIG = "sftp.writer.recordOutputTemplateValue";
  public static final String WRITER_RECORD_OUTPUT_TEMPLATE_VALUE_DEFAULT = "${value}";

  public static final String WRITER_RECORD_OUTPUT_TEMPLATE_ENABLED_CONFIG = "sftp.writer.recordOutputTemplate";
  public static final Boolean WRITER_RECORD_OUTPUT_TEMPLATE_ENABLED_DEFAULT = Boolean.FALSE;

  public static final String WRITER_RECORD_OUTPUT_DELIMETER_CONFIG = "sftp.writer.recordOutputDelimeter";
  public static final String WRITER_RECORD_OUTPUT_DELIMETER_DEFAULT = "\\n";

  public static final String WRITER_FLUSH_CACHE_SIZE_CONFIG = "sftp.writer.flushCacheSize";
  public static final int WRITER_FLUSH_CACHE_SIZE_DEFAULT = 2048;

  public static final String WRITER_WRITE_CHUNK_SIZE_CONFIG = "sftp.writer.writeChunkSize";
  public static final int WRITER_WRITE_CHUNK_SIZE_DEFAULT = 20480;

  public static final String WRITER_SLEEP_ON_FAIL_CONFIG = "sftp.writer.sleepOnFail";
  public static final long WRITER_SLEEP_ON_FAIL_DEFAULT = 60 * 1000;

  public static final String WRITER_RETRIES_ON_FAIL_CONFIG = "sftp.writer.retriesOnFail";
  public static final int WRITER_RETRIES_ON_FAIL_DEFAULT = 5;

  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();
    {
      configDef
          .define(HOST_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, null, Importance.HIGH, "SFTP host. Required.")
          .define(PORT_CONFIG, Type.INT, PORT_DEFAULT, null, Importance.HIGH, "SFTP port. Default: 22.")
          .define(USER_CONFIG, Type.STRING, null, null, Importance.HIGH, "SFTP user.")
          .define(PASSWORD_CONFIG, Type.PASSWORD, null, null, Importance.HIGH, "SFTP password.")
          .define(FINGERPRINT_CONFIG, Type.STRING, null, null, Importance.LOW, "Remote server fingerprint. Will be checked if specified")
          .define(AUTH_KEY_ALIAS_CONFIG, Type.STRING, null, null, Importance.MEDIUM, "Authentication key alias")

          .define(PATH_TEMPLATE_CONFIG, Type.STRING, PATH_TEMPLATE_DEFAULT, null, Importance.MEDIUM, "Path data will be stored")
          .define(FILE_TEMPLATE_CONFIG, Type.STRING, FILE_TEMPLATE_DEFAULT, null, Importance.MEDIUM, "Filename data will be stored")
          
          .define(POLL_INTERVAL_CONFIG, Type.LONG, POLL_INTERVAL_DEFAULT, null, Importance.MEDIUM, "If set will consume data periodically rather then realtime. Default: 0 - realtime")

          .define(WRITER_FLUSH_CACHE_SIZE_CONFIG, Type.INT, WRITER_FLUSH_CACHE_SIZE_DEFAULT, null, Importance.MEDIUM, "Max number of records to cache before flush")
          .define(WRITER_WRITE_CHUNK_SIZE_CONFIG, Type.INT, WRITER_WRITE_CHUNK_SIZE_DEFAULT, null, Importance.MEDIUM, "Size of chunk data will be written. Default: 20480")

          .define(WRITER_SLEEP_ON_FAIL_CONFIG, Type.LONG, WRITER_SLEEP_ON_FAIL_DEFAULT, null, Importance.LOW, "Time to wait on fail before retry. Default: 60 secs")
          .define(WRITER_RETRIES_ON_FAIL_CONFIG, Type.INT, WRITER_RETRIES_ON_FAIL_DEFAULT, null, Importance.LOW, "Number of retries after failure allowed. Default: 5")

          .define(WRITER_RECORD_OUTPUT_TEMPLATE_ENABLED_CONFIG, Type.BOOLEAN, WRITER_RECORD_OUTPUT_TEMPLATE_ENABLED_DEFAULT, null, Importance.MEDIUM, "Use Template for writing data to file. if disabled values only will be written")
          .define(WRITER_RECORD_OUTPUT_TEMPLATE_VALUE_CONFIG, Type.STRING, WRITER_RECORD_OUTPUT_TEMPLATE_VALUE_DEFAULT, null, Importance.MEDIUM, "Template how to write data to file")
          .define(WRITER_RECORD_OUTPUT_DELIMETER_CONFIG, Type.STRING, WRITER_RECORD_OUTPUT_DELIMETER_DEFAULT, null, Importance.MEDIUM, "Delimeter between records")

          .define(TMPFILE_MAX_INACTIVE_TIME_CONFIG, Type.LONG, TMPFILE_MAX_INACTIVE_TIME_DEFAULT, null, Importance.LOW, "If Use temporary files. Max time  without new data for file to be renamed")
          .define(TMPFILE_MAX_SIZE_CONFIG, Type.LONG, TMPFILE_MAX_SIZE_DEFAULT, null, Importance.LOW, "If Use temporary files. Max size for the file. Default: no limits.")
          
          .define(TMPPATH_TEMPLATE_CONFIG, Type.STRING, null, null, Importance.LOW, "Directory for temporary files. Will not be used if not defined")
          .define(TMPFILE_TEMPLATE_CONFIG, Type.STRING, null, null, Importance.LOW, "temporary files prefix. Will not be used if not defined")

          .withClientSslSupport();
    }

    return configDef;
  }

  public SftpSinkConfig(ConfigDef configDef, Map<?, ?> originals) {
    super(configDef, originals);
  }

  public SftpSinkConfig(Map<String, ?> props) {
    super(CONFIG_DEF, props);
  }

}
