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

import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveDataCommitter implements DataCommitter {

  public static DataCommitter FALLBACK_DATA_COMMITTER = new HiveDataCommitter();

  @Override
  public void moveFile(Path sourcePath, Path targetPath, boolean isDfsDir,
      boolean needCleanTarget, boolean isCTAS,
      HiveConf conf, SessionState.LogHelper console) throws HiveException {
    MoveTask.moveFile(sourcePath, targetPath, isDfsDir, needCleanTarget, isCTAS, conf, console);
  }

  @Override
  public void moveFile(Path tablePath, Path sourcePath, Path targetPath, boolean isDfsDir,
      boolean needCleanTarget, boolean isCTAS,
      HiveConf conf, SessionState.LogHelper console) throws HiveException {
    this.moveFile(sourcePath, targetPath, isDfsDir, needCleanTarget, isCTAS,  conf, console);
  }

  @Override
  public void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs,
                        boolean isSrcLocal, boolean isAcidIUD, boolean isOverwrite,
                        List<FileStatus> newFiles, boolean isBucketed, boolean isFullAcidTable,
                        boolean isManaged, boolean isCompactionTable) throws HiveException {
    Hive.copyFiles(conf, srcf, destf, fs,
        isSrcLocal, isAcidIUD, isOverwrite,
        newFiles, isBucketed, isFullAcidTable,
        isManaged, isCompactionTable);
  }

  @Override
  public void copyFiles(Path tablePath, HiveConf conf, Path srcf, Path destf, FileSystem fs,
                        boolean isSrcLocal, boolean isAcidIUD, boolean isOverwrite,
                        List<FileStatus> newFiles, boolean isBucketed, boolean isFullAcidTable,
                        boolean isManaged, boolean isCompactionTable) throws HiveException {
    this.copyFiles(conf, srcf, destf, fs, isSrcLocal, isAcidIUD, isOverwrite,
        newFiles, isBucketed, isFullAcidTable, isManaged, isCompactionTable);
  }

  @Override
  public void replaceFiles(Path tablePath, Path srcf, Path destf,
                           Path oldPath, HiveConf conf, boolean isSrcLocal,
                           boolean purge, List<FileStatus> newFiles,
                           PathFilter deletePathFilter, boolean isNeedRecycle,
                           boolean isManaged, boolean isInsertOverwrite, Hive hive) throws HiveException {
     hive.replaceFiles(tablePath, srcf, destf,
         oldPath, conf, isSrcLocal,
         purge, newFiles,
         deletePathFilter, isNeedRecycle,
         isManaged,
         isInsertOverwrite);
  }
}
