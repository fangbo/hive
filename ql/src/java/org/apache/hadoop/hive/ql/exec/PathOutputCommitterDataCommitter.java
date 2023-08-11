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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DataCommitter} that commits Hive data using a {@link PathOutputCommitter}.
 */
public class PathOutputCommitterDataCommitter implements DataCommitter {

  public static final Logger LOG = LoggerFactory.getLogger(PathOutputCommitterDataCommitter.class);
  private final JobContext jobContext;
  private final PathOutputCommitter pathOutputCommitter;
  private Set<String> writeIds = new HashSet<>();
  private Set<String> partNames;

  public PathOutputCommitterDataCommitter(JobContext jobContext, PathOutputCommitter pathOutputCommitter, Set<String> writeIds) {
    this.jobContext = jobContext;
    this.pathOutputCommitter = pathOutputCommitter;
    this.partNames = new HashSet<>();
    if (writeIds != null) {
      this.writeIds.addAll(writeIds) ;
    }
  }

  public Set<String> writeIds() {
    return writeIds;
  }

  public void moveFile(Path tablePath, Path sourcePath, Path targetPath, boolean isDfsDir, boolean needCleanTarget, boolean isCTAS,
      HiveConf conf, SessionState.LogHelper console) throws HiveException {
    commitJob(tablePath, targetPath);
  }

  @Override
  public void moveFile(Path sourcePath, Path targetPath, boolean isDfsDir, boolean needCleanTarget, boolean isCTAS, HiveConf conf,
                       SessionState.LogHelper console) throws HiveException {
    commitJob();
  }

  public void copyFiles(Path tablePath, HiveConf conf, Path srcf, Path destf, FileSystem fs, boolean isSrcLocal,
                        boolean isAcidIUD, boolean isOverwrite, List<FileStatus> newFiles, boolean isBucketed, boolean isFullAcidTable,
                        boolean isManaged, boolean isCompactionTable) throws HiveException {
    commitJob(tablePath, destf);
  }

  @Override
  public void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs, boolean isSrcLocal,
      boolean isAcidIUD, boolean isOverwrite, List<FileStatus> newFiles, boolean isBucketed, boolean isFullAcidTable,
      boolean isManaged, boolean isCompactionTable) throws HiveException {
    commitJob();
  }

  @Override
  public void replaceFiles(Path tablePath, Path srcf, Path destf, Path oldPath, HiveConf conf,
      boolean isSrcLocal, boolean purge, List<FileStatus> newFiles, PathFilter deletePathFilter, boolean isNeedRecycle,
      boolean isManaged, boolean isInsertOverwrite, Hive hive) throws HiveException {
    if (oldPath != null) {
      hive.deleteOldPathForReplace(destf, oldPath, conf, purge, deletePathFilter, isNeedRecycle);
    }
    commitJob(tablePath, destf);
  }

  public void cleanUpStagingFiles() {
    try {
      for (String writeId : writeIds) {
        CommitterManifest committerManifest = new CommitterManifest(jobContext, writeId);
        committerManifest.cleanupManifestDir();
      }
    } catch (IOException e) {
      throw new RuntimeException("delete manifest file error: ", e);
    }
  }

  private void commitJob(Path tablePath, Path destf) throws HiveException {
    if (Objects.equals(tablePath, destf)) {
      commitJob();
    } else {
      String partName = destf.toString();

      for (String writeId : writeIds) {
        CommitterManifest committerManifest = new CommitterManifest(jobContext, writeId);
        committerManifest.deleteLockFile(partName);
        if (!committerManifest.isLocked(partName)) {
          LOG.info("Part: {} not locked, commit job", partName);
          commitJob();
          break;
        } else {
          LOG.info("Part: {} has been locked, skip commit job", partName);
        }
      }
    }
  }

  private void commitJob() throws HiveException {
    try {
      this.pathOutputCommitter.commitJob(this.jobContext);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  public Path getOutputPath() {
    return pathOutputCommitter.getOutputPath();
  }

  public Path getWorkPath() throws IOException {
    return pathOutputCommitter.getWorkPath();
  }

  public JobContext jobContext() {
    return jobContext;
  }

  public Set<String> getPartNames() {
    if (!partNames.isEmpty()) {
      return partNames;
    }
    partNames = new HashSet<>();
    try {
      for (String writeId : writeIds) {
        CommitterManifest committerManifest = new CommitterManifest(jobContext, writeId);
        committerManifest.processManifestFiles().forEach(f -> partNames.add(f.partName()));
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Process manifest file error: ", e);
    }
    return partNames;
  }
}
