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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.FingerprintVerifier;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.UserAuthException;

public class SftpSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(SftpSinkConnector.class);

  private Map<String, String> configProps;

  protected static String version = "unknown";
  
  static {
    try {
      Properties props = new Properties();
      props.load(SftpSinkConnector.class.getResourceAsStream("/sftp-connector-version.properties"));
      version = props.getProperty("version", version).trim();
    } catch (Exception e) {
      log.warn("Error while loading version:", e);
    }

  }

  @Override
  public String version() {
    return version;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    configProps = props;
    SftpSinkConfig conf = new SftpSinkConfig(props);

    log.info("Check connection before starting tasks.");

    String fingerprint = conf.getString(SftpSinkConfig.FINGERPRINT_CONFIG);

    try (SSHClient ssh = new SSHClient();) {
      HostKeyVerifier hostVerifier = StringUtils.isNotBlank(fingerprint) ? FingerprintVerifier.getInstance(fingerprint) : new PromiscuousVerifier();
      ssh.addHostKeyVerifier(hostVerifier);
      ssh.connect(conf.getString(SftpSinkConfig.HOST_CONFIG), conf.getInt(SftpSinkConfig.PORT_CONFIG));
      String user = conf.getString(SftpSinkConfig.USER_CONFIG);
      Password pwd = conf.getPassword(SftpSinkConfig.PASSWORD_CONFIG);
      if (user != null && pwd != null) {
        ssh.authPassword(user, pwd.value());
      }
    } catch (UserAuthException e) {
      throw new ConnectException("SFTP auth failed", e);
    } catch (IOException e) {
      throw new ConnectException("Failed to connect to SFTP", e);
    }

    log.debug("Connector started.");

  }

  @Override
  public Class<? extends Task> taskClass() {
    return SftpSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  @Override
  public void stop() throws ConnectException {

  }

  @Override
  public ConfigDef config() {
    return SftpSinkConfig.CONFIG_DEF;
  }

}