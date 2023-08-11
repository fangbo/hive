/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CommitterManifest {
  private static final Logger LOG = LoggerFactory.getLogger(CommitterManifest.class);
  private static final String MANIFEST_DIR_PATH_KEY = "hive.optimized-committer.manifest.directory";
  private static final String DEFAULT_MANIFEST_DIR_PATH = ".pending-commits";
  private static final String SCRATCH_DIR_KEY = "hive.optimized-committer.scratch.directory";
  private static final String MR_JOB_APP_ATTEMPT_ID_KEY = "mapreduce.job.application.attempt.id";
  private static final int DEFAULT_MR_JOB_APP_ATTEMPT_ID = 0;
  private static final String MANIFEST_MARK = "__manifest";
  private static final String META_SUFFIX = ".meta";
  private static final String STATUS_SUFFIX = ".lock";
  private final Path manifestDir;
  private final Path lockDir;
  private final Configuration conf;
  private final String jobId;
  private final String writerId;

  private final Set<StagedFileMetadata> metadataSet = new HashSet<>();

  public CommitterManifest(JobContext context) {
    this.conf = context.getConfiguration();
    this.jobId = context.getJobID().toString();
    this.writerId = "";
    this.manifestDir = buildManifestDir();
    this.lockDir = manifestDir.getParent();
    LOG.info("Init manifest directory: {} with jobContext", manifestDir);
  }

  public CommitterManifest(TaskAttemptContext context, String writerId) {
    this.conf = context.getConfiguration();
    this.jobId = context.getJobID().toString();
    this.writerId = writerId;
    this.manifestDir = buildManifestDir();
    this.lockDir = manifestDir.getParent();
    LOG.info("Init manifest directory: {} with taskContext", manifestDir);
  }

  public CommitterManifest(JobContext context, String writerId) {
    this.conf = context.getConfiguration();
    this.jobId = context.getJobID().toString();
    this.writerId = writerId;
    this.manifestDir = buildManifestDir();
    this.lockDir = manifestDir.getParent();
    LOG.info("Init manifest directory: {} with jobContext", manifestDir);
  }

  public void addMetadata(StagedFileMetadata metadata) {
    LOG.info("Add new part name into manifest dir: {}", metadata.partName);
    metadataSet.add(metadata);
  }

  public Set<StagedFileMetadata> processManifestFiles() throws IOException, ClassNotFoundException {
    FileSystem fs = manifestDir.getFileSystem(conf);
    Set<StagedFileMetadata> metadataList = new HashSet<>();
    if (!fs.exists(manifestDir)) {
      LOG.info("ManifestDir not exist {}", manifestDir);
      return metadataList;
    }
    RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(manifestDir, true);
    while (fileStatusRemoteIterator.hasNext()) {
      LocatedFileStatus status = fileStatusRemoteIterator.next();
      if (status.getPath().getName().endsWith(META_SUFFIX)) {
        try (FSDataInputStream fis = fs.open(status.getPath());
             ObjectInputStream ois = new ObjectInputStream(fis)) {
          while(fis.available() > 0) {
            StagedFileMetadata metadata = (StagedFileMetadata)ois.readObject();
            if (metadata == null) {
              throw new IOException("The meta file is NULL, the file may have been corrupted.");
            }
            if (!Objects.equals(metadata.writerId, writerId)) {
              throw new IOException(String.format("The meta file does not belong to the writer id, except: %s ,but got: %s, partName: %s", writerId, metadata.writerId, metadata.partName));
            }
            metadataList.add(metadata);
            LOG.info("Load part name: {} under path: {}, writerId: {}", metadata.partName, manifestDir, writerId);
          }
        }
      }
    }
    return metadataList;
  }

  public void persistMetaData() throws IOException {
    if (metadataSet.isEmpty()) {
      return;
    }
    Path manifestFile = this.getManifestFile();
    FileSystem fs = this.manifestDir.getFileSystem(this.conf);
    try (ObjectOutputStream oos = new ObjectOutputStream(fs.create(manifestFile))) {
      for (StagedFileMetadata metadata: metadataSet) {
        LOG.info("Write part name: {}", metadata.partName);
        oos.writeObject(metadata);
      }
    }
  }

  public void deleteLockFile(String partName) {
    try {
      FileSystem fs = lockDir.getFileSystem(conf);
      fs.delete(createLockFilePath(partName), true);
      LOG.info("Delete lock file: {}", createLockFilePath(partName));
    } catch (IOException e) {
      LOG.error("Delete lock file: {} error: {}", writerId, e);
    }
  }

  public void createLockFile(String partName) {
    createLockFile(createLockFilePath(partName));
  }

  public boolean isLocked(String partName) {
    try {
      FileSystem fs = lockDir.getFileSystem(conf);
      if (!fs.exists(lockDir)) {
        LOG.info("lockDir not exist {}", lockDir);
        return false;
      }
      FileStatus[] fileStatuses = fs.listStatus(lockDir, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().startsWith(normalizedPartName(partName))
              && path.getName().endsWith(STATUS_SUFFIX)
              && !Objects.equals(path, createLockFilePath(partName));
        }
      });
      LOG.info("partName: {}, writerId:{}, lock: {}", partName, writerId, fileStatuses.length > 0);
      return fileStatuses.length > 0;
    } catch (Exception e) {
      LOG.error("Get locked status error, partName: {}, writerId:{}", partName, writerId, e);
      return false;
    }
  }

  private String normalizedPartName(String partName) {
    return partName.replaceAll("/", "_").replaceAll("=", "").replaceAll(":", "");
  }

  private Path createLockFilePath(String partName) {
    return new Path(lockDir, String.format("%s_%s%s", normalizedPartName(partName), writerId, STATUS_SUFFIX));
  }

  private void createLockFile(Path lockFile) {
    try {
      FileSystem fs = lockFile.getFileSystem(conf);
      try (ObjectOutputStream oos = new ObjectOutputStream(fs.create(lockFile))) {
        oos.writeObject(lockFile.toString());
        LOG.info("Write lock file: {}", lockFile);
      }
    } catch (IOException e) {
      LOG.error("Create lock file error: ", e);
    }
  }

  private Path getManifestFile() {
    return new Path(manifestDir, String.format("%s.%s", UUID.randomUUID(), META_SUFFIX));
  }

  private Path buildManifestDir() {
    Path path = new Path(getScratchDir(conf), conf.get(MANIFEST_DIR_PATH_KEY, DEFAULT_MANIFEST_DIR_PATH));
    return new Path(path, jobId + "/" + writerId);
  }

  private Path getScratchDir(Configuration conf) {
    String pathStr = conf.get("fs.defaultFS");
    if (!pathStr.endsWith("/")) {
      pathStr = pathStr + "/";
    }
    pathStr = pathStr + MANIFEST_MARK + "/" + getAppAttemptId(conf);
    return new Path(conf.get(SCRATCH_DIR_KEY, pathStr));
  }

  private int getAppAttemptId(Configuration conf) {
    return conf.getInt(MR_JOB_APP_ATTEMPT_ID_KEY, DEFAULT_MR_JOB_APP_ATTEMPT_ID);
  }

  public void cleanupManifestDir() throws IOException {
    FileSystem fs = manifestDir.getFileSystem(conf);
    if (fs.exists(manifestDir)) {
      LOG.info("Cleaning up manifest directory: {}", manifestDir);
      fs.delete(manifestDir, true);
    }
  }
  public static class StagedFileMetadata implements Serializable {
    private String partName;
    private String writerId;

    public StagedFileMetadata(String partName, String writerId) {
      Preconditions.checkNotNull(partName, "partName cannot be null");
      Preconditions.checkNotNull(writerId, "writerId cannot be null");
      this.partName = partName;
      this.writerId = writerId;
    }

    public String partName() {
      return partName;
    }

    public String writerId() {
      return writerId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(partName, writerId);
    }

    @Override
    public boolean equals(Object obj){
      if (!(obj instanceof StagedFileMetadata)) {
        return false;
      }
      StagedFileMetadata metadata = (StagedFileMetadata) obj;
      return Objects.equals(partName, metadata.partName)
          && Objects.equals(writerId, metadata.writerId);
    }
  }
}
