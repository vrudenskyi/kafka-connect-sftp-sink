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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SftpFile {

  private final String path;
  private boolean pathCreated = false;
  private final String fileName;
  private String tmpPath;
  private boolean tmpPathCreated = false;
  private String tmpFileName;
  private boolean useTmp;
  private long lastWriteTime = 0L;

  public SftpFile(String path, String fileName, String tmpPath, String tmpFileName) {
    this.path = path;
    this.fileName = fileName;

    if (StringUtils.isBlank(path) || StringUtils.isBlank(fileName)) {
      throw new IllegalArgumentException("Both path and filename required" + path + " - " + fileName);
    }

    this.useTmp = StringUtils.isNotBlank(tmpPath) || StringUtils.isNotBlank(tmpFileName);
    this.tmpPath = StringUtils.isNotBlank(tmpPath) ? tmpPath : path;
    this.tmpFileName = StringUtils.isNotBlank(tmpFileName) ? tmpFileName : fileName;
  }

  public String getFullTmpPath() {
    if (tmpPath.endsWith("/")) {
      return tmpPath + tmpFileName;
    }
    return tmpPath + "/" + tmpFileName;
  }

  public String getFullPath() {
    if (path.endsWith("/")) {
      return path + fileName;
    }
    return path + "/" + fileName;
  }

  public String getPath() {
    return path;
  }

  public String getTmpPath() {
    return tmpPath;
  }

  public String getFileName() {
    return fileName;
  }

  public long getLastWriteTime() {
    return lastWriteTime;
  }

  public void setLastWriteTime(long lastWriteTime) {
    this.lastWriteTime = lastWriteTime;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SftpFile) {
      final SftpFile other = (SftpFile) obj;
      return new EqualsBuilder()
          .append(path, other.path)
          .append(fileName, other.fileName)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(path)
        .append(fileName)
        .toHashCode();
  }

  public boolean isUseTmp() {
    return useTmp;
  }

  public boolean isPathCreated() {
    return pathCreated;
  }

  public boolean isTmpPathCreated() {
    return tmpPathCreated;
  }

  public void setPathCreated(boolean pathCreated) {
    this.pathCreated = pathCreated;
  }

  public void setTmpPathCreated(boolean tmpPathCreated) {
    this.tmpPathCreated = tmpPathCreated;
  }

}
