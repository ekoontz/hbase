package org.apache.hadoop.hbase.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/*
 * Copyright 2010 The Apache Software Foundation
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

public class HDFSDirectory extends Directory {
  FileSystem fileSystem;
  String path;
  
  public HDFSDirectory(FileSystem fileSystem, String path) throws IOException {
    this.fileSystem = fileSystem;
    this.path = path;
    setLockFactory(new HDFSLockFactory(path, fileSystem));
  }
  
  public IndexOutput createOutput(String name) throws IOException {
    System.out.println("createOutput:"+name);
    return new HDFSIndexOutput(getPath(name));
  }
  
  // TODO: we need to reuse the underlying FSDataOutputStream
  // across opens to the same file
  protected class HDFSIndexOutput extends IndexOutput {
    Path path;
    FSDataOutputStream output;
    
    public HDFSIndexOutput(Path path) throws IOException {
      this.path = path;
      output = fileSystem.create(path);
    }
    
    public void writeByte(byte b) throws IOException {
      output.write(b);
    }
    
    public void writeBytes(byte[] b, int off, int len) throws IOException {
      output.write(b, off, len);
    }
    
    public void flush() throws IOException {
      output.flush();
    }
    
    public void close() throws IOException {
      output.close();
    }
    
    public long getFilePointer() {
      try {
        return output.getPos();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    
    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public long length() throws IOException {
      return output.getPos();
    }
    
    @Override
    public void setLength(long length) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
  
  public void close() throws IOException {
    fileSystem.close();
  }
  
  public String[] listAll() throws IOException {
    List<String> files = new ArrayList<String>();
    FileStatus[] statuses = fileSystem.listStatus(new Path(path));
    for (FileStatus status : statuses) {
      files.add(status.getPath().getName());
    }
    return (String[])files.toArray(new String[0]);
  }
  
  public void sync(Collection<String> names) throws IOException {}
  
  public long fileLength(String name) throws IOException {
    return fileSystem.getFileStatus(new Path(path, name)).getLen();
  }
  
  public boolean fileExists(String name) throws IOException {
    return fileSystem.exists(new Path(path, name));
  }
  
  public long fileModified(String name) throws IOException {
    return fileSystem.getFileStatus(new Path(path, name))
        .getModificationTime();
  }
  
  public void touchFile(String name) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public void deleteFile(String name) throws IOException {
    Path path = getPath(name);
    boolean deleted = fileSystem.delete(path, false);
    System.out.println("deleteFile:"+name+" "+deleted);
    assert deleted;
  }
  
  private Path getPath(String name) {
    return new Path(path + "/" + name);
  }
  
  protected class HDFSIndexInput extends BufferedIndexInput {
    Path path;
    FSDataInputStream input;
    long position;
    FileStatus fileStatus;
    boolean isClone;
    
    public HDFSIndexInput(Path path, int bufferSize) throws IOException {
      super(bufferSize);
      this.path = path;
      System.out.println("open input:"+path);
      input = fileSystem.open(path, bufferSize);
      fileStatus = fileSystem.getFileStatus(path);
    }
    
    protected void readInternal(byte[] b, int offset, int length)
        throws IOException {
      try {
        long position = getFilePointer();
        input.read(position, b, offset, length);
      } catch (IOException ioe) {
        throw new IOException(path+"", ioe);
      }
    }
    
    @Override
    public Object clone() {
      HDFSIndexInput clone = (HDFSIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
    
    public void close() throws IOException {
      if (!isClone) {
        System.out.println("close:"+path);
        input.close();
      }
    }
    
    protected void seekInternal(long pos) throws IOException {
      this.position = pos;
    }
    
    public long length() {
      return fileStatus.getLen();
    }
  }
  
  public IndexInput openInput(String name) throws IOException {
    return openInput(name, BufferedIndexInput.BUFFER_SIZE);
  }
  
  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    Path path = getPath(name);
    return new HDFSIndexInput(path, bufferSize);
  }
}
