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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.BlobStorageUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverMergeFiles;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PathOutputCommitterWork;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PathOutputCommitterResolver implements PhysicalPlanResolver {

  private static final Logger LOG = LoggerFactory.getLogger(PathOutputCommitterResolver.class);

  // All FileSinkOperators
  private final Map<Task<?>, Collection<FileSinkOperator>> taskToFsOps = new HashMap<>();

  private final List<Task<MoveWork>> mvTasks = new ArrayList<>();

  private final Map<Task<?>, ConditionalTask> tasksInConditionalNotUsed = new HashMap<>();

  private HiveConf hconf;

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    this.hconf = pctx.getConf();

    // Collect all MoveTasks and FSOPs
    TaskGraphWalker graphWalker = new TaskGraphWalker(new PathOutputCommitterDispatcher());
    List<Node> rootTasks = new ArrayList<>(pctx.getRootTasks());
    graphWalker.startWalking(rootTasks, null);

    // Find MoveTasks with no child MoveTask
    List<Task<MoveWork>> sinkMoveTasks = mvTasks.stream()
        .filter(mvTask -> !containsChildTask(mvTask.getChildTasks(), MoveTask.class))
        .filter(mvTask -> !tasksInConditionalNotUsed.containsKey(mvTask))
        .collect(Collectors.toList());

    // Iterate through each FSOP
    for (Map.Entry<Task<?>, Collection<FileSinkOperator>> entry : taskToFsOps.entrySet()) {
      for (FileSinkOperator fsOp : entry.getValue()) {
        try {
          processFsOp(pctx, entry.getKey(), fsOp, sinkMoveTasks);
        } catch (HiveException | MetaException e) {
          throw new SemanticException(e);
        }
      }
      for (FileSinkOperator fsOp : entry.getValue()) {
        try {
          insertCommitterSetupTask(pctx, entry.getKey(), fsOp);
        } catch (HiveException | MetaException e) {
          throw new SemanticException(e);
        }
      }
    }

    return pctx;
  }

  private boolean containsChildTask(List<Task<? extends Serializable>> mvTasks, Class<MoveTask>
      taskClass) {
    if (mvTasks == null) {
      return false;
    }
    boolean containsChildTask = false;
    for (Task<? extends Serializable> mvTask : mvTasks) {
      if (taskClass.isInstance(mvTask)) {
        return true;
      }
      containsChildTask = containsChildTask(mvTask.getChildTasks(), taskClass);
    }
    return containsChildTask;
  }

  private class PathOutputCommitterDispatcher implements SemanticDispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack,
        Object... nodeOutputs) throws SemanticException {

      Task<?> task = (Task<?>) nd;

      if (task instanceof ConditionalTask) {
        ConditionalTask ctask = (ConditionalTask)task;
        if (ctask.getResolverCtx() instanceof ConditionalResolverMergeFiles.ConditionalResolverMergeFilesCtx) {
          ConditionalResolverMergeFiles.ConditionalResolverMergeFilesCtx ctx =
              (ConditionalResolverMergeFiles.ConditionalResolverMergeFilesCtx) (ctask.getResolverCtx());

          tasksInConditionalNotUsed.put(ctx.getListTasks().get(1), ctask);
          tasksInConditionalNotUsed.put(ctx.getListTasks().get(0), ctask);
          tasksInConditionalNotUsed.put(ctx.getListTasks().get(2), ctask);
        }
      } else if (task instanceof MoveTask) {
        mvTasks.add((MoveTask) nd);
      } else {
        Collection<FileSinkOperator> fsOps = getAllFsOps(task);
        if (!fsOps.isEmpty()) {
          taskToFsOps.put((Task<?>) nd, fsOps);
        }
      }

      return null;
    }
  }

  public static Collection<FileSinkOperator> getAllFsOps(Task<?> task) {
    Collection<Operator<?>> fsOps = new ArrayList<>();
    if (task instanceof MapRedTask) {
      fsOps.addAll(((MapRedTask) task).getWork().getAllOperators());
    } else if (task instanceof TezTask) {
      fsOps.addAll(((TezTask) task).getWork()
          .getAllWork()
          .stream()
          .flatMap(t -> t.getAllOperators().stream())
          .collect(Collectors.toList()));
    }
    return fsOps.stream()
        .filter(FileSinkOperator.class::isInstance)
        .map(FileSinkOperator.class::cast)
        .collect(Collectors.toList());
  }

  private void processFsOp(
      PhysicalContext pctx,
      Task<?> task, FileSinkOperator fsOp,
      List<Task<MoveWork>> sinkMoveTasks) throws HiveException, MetaException {
    if (tasksInConditionalNotUsed.containsKey(task)) {
      return;
    }

    FileSinkDesc fileSinkDesc = fsOp.getConf();

    // Get the MoveTask that will process the output of the fsOp
    Task<MoveWork> mvTask = GenMapRedUtils.findMoveTaskForFsopOutput(
        sinkMoveTasks,
        fileSinkDesc.getFinalDirName(),
        fileSinkDesc.isMmTable(),
        false,
        fileSinkDesc.getMoveTaskId(),
        AcidUtils.Operation.NOT_ACID);

    MoveWork mvWork = null;

    // The output path which job committer to commit to.
    Path outputPath = null;

    if (mvTask != null) {
      mvWork = mvTask.getWork();

      // Instead of picking between load table work and load file work, throw an exception if
      // they are both set (this should never happen)
      if (mvWork.getLoadTableWork() != null && mvWork.getLoadFileWork() != null) {
        throw new IllegalArgumentException("Load Table Work and Load File Work cannot both be set");
      }

      // If there is a load file work, get its output path
      if (mvWork.getLoadFileWork() != null) {
        outputPath = getLoadFileOutputPath(mvWork);
      }

      // If there is a load table work, get is output path
      if (mvTask.getWork().getLoadTableWork() != null) {
        outputPath = getLoadTableOutputPath(mvWork, fileSinkDesc);
      }
    } else {
      outputPath = fileSinkDesc.getFinalDirName();
    }

    if (outputPath == null) {
      LOG.info("There is no output path for file sink: {}", fileSinkDesc.getDirName());
      return;
    }

    if (!BlobStorageUtils.isBlobStorageCommitterEnabled(hconf, outputPath)) {
      LOG.info("The output path:{} is not support job committer for file sink: {}",
          outputPath, fileSinkDesc.getFinalDirName());
      return;
    }

    PathOutputCommitterWork outputCommitterWork = createPathOutputCommitterWork(hconf, outputPath);
    outputCommitterWork.addWriterId(fileSinkDesc.writerId());

    if (mvWork != null) {
      if (mvWork.getPathOutputCommitterWork() != null) {
        mvWork.getPathOutputCommitterWork().addWriterId(fileSinkDesc.writerId());
      } else {
        mvWork.setPathOutputCommitterWork(outputCommitterWork);
      }
      fileSinkDesc.setCommitJobWhenJobCloseOp(false);
      LOG.info("Using Output Committer: {} for MoveTask:{}, FileSinkOperator:{}, output path:{}",
          outputCommitterWork.getPathOutputCommitterClass(), mvTask, fsOp, outputPath);
    } else {
      fileSinkDesc.setCommitJobWhenJobCloseOp(true);
      LOG.info("Using Output Committer:{} for FileSinkOperator:{}, output path:{}  at jobCloseOp.",
          outputCommitterWork.getPathOutputCommitterClass(), fsOp, outputPath);
    }

    fileSinkDesc.setHasOutputCommitter(true);
    fileSinkDesc.setOutputPath(outputPath.toString());
  }

  private void insertCommitterSetupTask(
      PhysicalContext pctx,
      Task<?> task, FileSinkOperator fsOp) throws HiveException, MetaException {

    if (!fsOp.getConf().isHasOutputCommitter()) {
      return;
    }
    PathOutputCommitterWork committerWork = fsOp.getConf().getOutputCommitterWork(hconf);
    Task<?> committerSetupTask = TaskFactory.get(committerWork);
    insertCommitterSetupTaskBeforeTask(pctx, task, committerSetupTask);
  }

  /**
   * Suppose
   * @param pctx
   * @param task
   * @param setupTask
   */
  private void insertCommitterSetupTaskBeforeTask(PhysicalContext pctx, Task<?> task, Task<?> setupTask) {
    List<Task<? extends Serializable>> parentTasks = task.getParentTasks();
    task.setParentTasks(null);
    if (parentTasks != null) {
      for (Task<? extends Serializable> tsk : parentTasks) {
        tsk.addDependentTask(setupTask);
        tsk.removeDependentTask(task);
      }
    } else {
      if (pctx.getRootTasks().contains(task)) {
        pctx.removeFromRootTask(task);
        pctx.addToRootTask(setupTask);
      }
    }

    setupTask.addDependentTask(task);
  }

  public static PathOutputCommitterWork createPathOutputCommitterWork(Configuration hconf, Path outputPath) {
    JobID jobID = new JobID(HiveConf.getVar(hconf, HiveConf.ConfVars.HIVEQUERYID), 0);
    TaskAttemptContext taskAttemptContext = createTaskAttemptContext(hconf, jobID);
    JobContext jobContext = new JobContextImpl(hconf, jobID);

    return new PathOutputCommitterWork(outputPath.toString(),
        jobContext, taskAttemptContext);
  }

  // Somewhat copied from TaskCompiler, which uses similar logic to get the default location for
  // the target table in a CTAS query
  private Path getDefaultPartitionPath(Path tablePath, Map<String, String> partitionSpec)
      throws MetaException {
    Warehouse wh = new Warehouse(hconf);
    return wh.getPartitionPath(tablePath, partitionSpec);
  }

  public static TaskAttemptContext createTaskAttemptContext(Configuration hconf, JobID jobID) {
    return new TaskAttemptContextImpl(hconf,
        new TaskAttemptID(jobID.getJtIdentifier(), jobID.getId(), TaskType.JOB_SETUP, 0, 0));
  }

  private Path getLoadFileOutputPath(MoveWork mvWork) {
    return mvWork.getLoadFileWork().getTargetDir();
  }

  private Path getLoadTableOutputPath(MoveWork mvWork, FileSinkDesc fileSinkDesc) throws HiveException, MetaException {
    if (mvWork.getLoadTableWork().getDPCtx() != null) {
      return getLoadDynamicPartitionOutputPath(fileSinkDesc, mvWork);
    }

    if (mvWork.getLoadTableWork().getPartitionSpec() != null &&
        !mvWork.getLoadTableWork().getPartitionSpec().isEmpty()) {
      return getLoadPartitionOutputPath(mvWork);
    }

    // should probably replace this with Hive.getTable().getDataLocation()
    return new Path(mvWork.getLoadTableWork().getTable().getProperties()
        .getProperty("location")); // INSERT INTO ... VALUES (...)
  }

  private Path getLoadPartitionOutputPath(MoveWork mvWork) throws HiveException, MetaException {
    Hive db = Hive.get();
    Partition partition = db.getPartition(db.getTable(mvWork.getLoadTableWork()
            .getTable().getTableName()),
        mvWork.getLoadTableWork().getPartitionSpec(), false);
    if (partition != null) {
      return partition.getDataLocation();
    } else {
      return getDefaultPartitionPath(db.getTable(mvWork.getLoadTableWork()
          .getTable().getTableName()).getDataLocation(), mvWork
          .getLoadTableWork().getPartitionSpec());
    }
  }

  private Path getLoadDynamicPartitionOutputPath(FileSinkDesc fileSinkDesc, MoveWork mvWork) throws HiveException, MetaException {
    String tableLocation = fileSinkDesc.getTableInfo().getProperties().getProperty("location");

    if (mvWork.getLoadTableWork().getDPCtx().getSPPath() != null
        && !mvWork.getLoadTableWork().getDPCtx().getSPPath().isEmpty()) {
      return new Path(tableLocation, mvWork.getLoadTableWork().getDPCtx().getSPPath());
    }
    return new Path(tableLocation);
  }
}
