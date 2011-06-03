package org.apache.hadoop.hbase.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockReleaseFailedException;

/**
 * HDFSLockFactory is similar to FSLockFactory.
 */
public class HDFSLockFactory extends LockFactory {
  String lockDir;
  FileSystem fileSystem;

  public HDFSLockFactory() throws IOException {
  }
  
  public HDFSLockFactory(String lockDir, FileSystem fileSystem) throws IOException {
    this.lockDir = lockDir;
    this.fileSystem = fileSystem;
  }

  @Override
  public Lock makeLock(String lockName) {
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    return new HDFSLock(lockDir, lockName, fileSystem);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    if (fileSystem.exists(new Path(lockDir))) {
      if (lockPrefix != null) {
        lockName = lockPrefix + "-" + lockName;
      }
      Path path = new Path(lockDir, lockName);
      if (fileSystem.exists(path) && !fileSystem.delete(path)) {
        throw new IOException("Cannot delete " + path);
      }
    }
  }
  
  public static class HDFSLock extends Lock {
    String lockFile;
    String lockDir;
    FileSystem fileSystem;
    Path lockPath;

    public HDFSLock(String lockDir, String lockFileName, FileSystem fileSystem) {
      this.lockDir = lockDir;
      lockFile = lockDir+"/"+lockFileName;
      this.fileSystem = fileSystem;
      lockPath = new Path(lockDir, lockFile);
    }

    @Override
    public boolean obtain() throws IOException {
      Path lockPath = new Path(lockDir, lockFile);
      // Ensure that lockDir exists and is a directory:
      if (!fileSystem.exists(lockPath)) {
        if (!fileSystem.mkdirs(new Path(lockDir))) {
          throw new IOException("Cannot create directory: " + lockPath);
        
        } else if (!fileSystem.isDirectory(new Path(lockDir))) {//lockDir.) {//isDirectory()) {
          throw new IOException("Found regular file where directory expected: " + 
                              lockDir);
        }
        FSDataOutputStream out = fileSystem.create(lockPath);
        out.write(0);
        out.close();
        return true;
      }
      return false;
    }

    @Override
    public void release() throws IOException, LockReleaseFailedException {
      if (fileSystem.exists(lockPath) && !fileSystem.delete(lockPath))
        throw new LockReleaseFailedException("failed to delete " + lockFile);
    }

    @Override
    public boolean isLocked() throws IOException {
      return fileSystem.exists(lockPath);
    }

    @Override
    public String toString() {
      return "HDFSLock@" + lockFile;
    }
  }
}