/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.wal.checkpoint.Checkpoint;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** CheckpointWriter writes the binary {@link Checkpoint} into .checkpoint file. */
public class CheckpointWriter extends LogWriter {
  public static final String FILE_SUFFIX = IoTDBConstant.WAL_CHECKPOINT_FILE_SUFFIX;
  public static final Pattern CHECKPOINT_FILE_NAME_PATTERN =
      Pattern.compile("_(?<versionId>\\d+)\\.checkpoint");

  /** Return true when this file is .checkpoint file */
  public static boolean checkpointFilenameFilter(File dir, String name) {
    return CHECKPOINT_FILE_NAME_PATTERN.matcher(name).find();
  }

  /**
   * Parse version id from filename
   *
   * @return Return {@link Integer#MIN_VALUE} when this file is not .checkpoint file
   */
  public static int parseVersionId(String filename) {
    Matcher matcher = CHECKPOINT_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group("versionId"));
    }
    return Integer.MIN_VALUE;
  }

  /** Get .checkpoint filename */
  public static String getLogFileName(long version) {
    return FILE_PREFIX + version + FILE_SUFFIX;
  }

  public CheckpointWriter(File logFile) throws FileNotFoundException {
    super(logFile);
  }
}
