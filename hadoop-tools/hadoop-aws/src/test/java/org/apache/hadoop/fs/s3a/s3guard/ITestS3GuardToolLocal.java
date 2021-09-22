/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Diff;

import static org.apache.hadoop.fs.s3a.MultipartTestUtils.*;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test S3Guard related CLI commands against a LocalMetadataStore.
 * Also responsible for testing the non s3guard-specific commands that, for
 * now, live under the s3guard CLI command.
 */
public class ITestS3GuardToolLocal extends AbstractS3GuardToolTestBase {

  private static final String LOCAL_METADATA = "local://metadata";
  private static final String[] ABORT_FORCE_OPTIONS = new String[] {"-abort",
      "-force", "-verbose"};

  @Override
  protected MetadataStore newMetadataStore() {
    return new LocalMetadataStore();
  }

  @Test
  public void testImportCommand() throws Exception {
    S3AFileSystem fs = getFileSystem();
    MetadataStore ms = getMetadataStore();
    Path parent = path("test-import");
    fs.mkdirs(parent);
    Path dir = new Path(parent, "a");
    fs.mkdirs(dir);
    Path emptyDir = new Path(parent, "emptyDir");
    fs.mkdirs(emptyDir);
    for (int i = 0; i < 10; i++) {
      String child = String.format("file-%d", i);
      try (FSDataOutputStream out = fs.create(new Path(dir, child))) {
        out.write(1);
      }
    }

    S3GuardTool.Import cmd = new S3GuardTool.Import(fs.getConf());
    cmd.setStore(ms);
    exec(cmd, "import", parent.toString());

    DirListingMetadata children =
        ms.listChildren(dir);
    assertEquals("Unexpected number of paths imported", 10, children
        .getListing().size());
    assertEquals("Expected 2 items: empty directory and a parent directory", 2,
        ms.listChildren(parent).getListing().size());
    // assertTrue(children.isAuthoritative());
  }

  @Test
  public void testDiffCommand() throws Exception {
    S3AFileSystem fs = getFileSystem();
    MetadataStore ms = getMetadataStore();
    Set<Path> filesOnS3 = new HashSet<>(); // files on S3.
    Set<Path> filesOnMS = new HashSet<>(); // files on metadata store.

    Path testPath = path("test-diff");
    mkdirs(testPath, true, true);

    Path msOnlyPath = new Path(testPath, "ms_only");
    mkdirs(msOnlyPath, false, true);
    filesOnMS.add(msOnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(msOnlyPath, String.format("file-%d", i));
      createFile(file, false, true);
      filesOnMS.add(file);
    }

    Path s3OnlyPath = new Path(testPath, "s3_only");
    mkdirs(s3OnlyPath, true, false);
    filesOnS3.add(s3OnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(s3OnlyPath, String.format("file-%d", i));
      createFile(file, true, false);
      filesOnS3.add(file);
    }

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    Diff cmd = new Diff(fs.getConf());
    cmd.setStore(ms);
    exec(cmd, buf, "diff", "-meta", LOCAL_METADATA,
            testPath.toString());

    Set<Path> actualOnS3 = new HashSet<>();
    Set<Path> actualOnMS = new HashSet<>();
    boolean duplicates = false;
    try (BufferedReader reader =
             new BufferedReader(new InputStreamReader(
                 new ByteArrayInputStream(buf.toByteArray())))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] fields = line.split("\\s");
        assertEquals("[" + line + "] does not have enough fields",
            4, fields.length);
        String where = fields[0];
        Path path = new Path(fields[3]);
        if (Diff.S3_PREFIX.equals(where)) {
          duplicates = duplicates || actualOnS3.contains(path);
          actualOnS3.add(path);
        } else if (Diff.MS_PREFIX.equals(where)) {
          duplicates = duplicates || actualOnMS.contains(path);
          actualOnMS.add(path);
        } else {
          fail("Unknown prefix: " + where);
        }
      }
    }
    String actualOut = buf.toString();
    assertEquals("Mismatched metadata store outputs: " + actualOut,
        filesOnMS, actualOnMS);
    assertEquals("Mismatched s3 outputs: " + actualOut, filesOnS3, actualOnS3);
    assertFalse("Diff contained duplicates", duplicates);
  }

  @Test
  public void testDestroyBucketExistsButNoTable() throws Throwable {
    run(Destroy.NAME,
        "-meta", LOCAL_METADATA,
        getLandsatCSVFile());
  }

  @Test
  public void testImportNoFilesystem() throws Throwable {
    final Import importer =
        new S3GuardTool.Import(getConfiguration());
    importer.setStore(getMetadataStore());
    intercept(IOException.class,
        new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return importer.run(
                new String[]{
                    "import",
                    "-meta", LOCAL_METADATA,
                    S3A_THIS_BUCKET_DOES_NOT_EXIST
                });
          }
        });
  }

  @Test
  public void testInfoBucketAndRegionNoFS() throws Throwable {
    intercept(FileNotFoundException.class,
        new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return run(BucketInfo.NAME, "-meta",
                LOCAL_METADATA, "-region",
                "any-region", S3A_THIS_BUCKET_DOES_NOT_EXIST);
          }
        });
  }

  @Test
  public void testInitNegativeRead() throws Throwable {
    runToFailure(INVALID_ARGUMENT,
        Init.NAME, "-meta", LOCAL_METADATA, "-region",
        "eu-west-1",
        READ_FLAG, "-10");
  }

  @Test
  public void testInit() throws Throwable {
    run(Init.NAME,
        "-meta", LOCAL_METADATA,
        "-region", "us-west-1");
  }

  @Test
  public void testInitTwice() throws Throwable {
    run(Init.NAME,
        "-meta", LOCAL_METADATA,
        "-region", "us-west-1");
    run(Init.NAME,
        "-meta", LOCAL_METADATA,
        "-region", "us-west-1");
  }

  @Test
  public void testLandsatBucketUnguarded() throws Throwable {
    run(BucketInfo.NAME,
        "-" + BucketInfo.UNGUARDED_FLAG,
        getLandsatCSVFile());
  }

  @Test
  public void testLandsatBucketRequireGuarded() throws Throwable {
    runToFailure(E_BAD_STATE,
        BucketInfo.NAME,
        "-" + BucketInfo.GUARDED_FLAG,
        ITestS3GuardToolLocal.this.getLandsatCSVFile());
  }

  @Test
  public void testLandsatBucketRequireUnencrypted() throws Throwable {
    run(BucketInfo.NAME,
        "-" + BucketInfo.ENCRYPTION_FLAG, "none",
        getLandsatCSVFile());
  }

  @Test
  public void testLandsatBucketRequireEncrypted() throws Throwable {
    runToFailure(E_BAD_STATE,
        BucketInfo.NAME,
        "-" + BucketInfo.ENCRYPTION_FLAG,
        "AES256", ITestS3GuardToolLocal.this.getLandsatCSVFile());
  }

  @Test
  public void testStoreInfo() throws Throwable {
    S3GuardTool.BucketInfo cmd = new S3GuardTool.BucketInfo(
        getFileSystem().getConf());
    cmd.setStore(getMetadataStore());
    String output = exec(cmd, cmd.getName(),
        "-" + S3GuardTool.BucketInfo.GUARDED_FLAG,
        getFileSystem().getUri().toString());
    LOG.info("Exec output=\n{}", output);
  }

  @Test
  public void testSetCapacity() throws Throwable {
    S3GuardTool cmd = new S3GuardTool.SetCapacity(getFileSystem().getConf());
    cmd.setStore(getMetadataStore());
    String output = exec(cmd, cmd.getName(),
        "-" + READ_FLAG, "100",
        "-" + WRITE_FLAG, "100",
        getFileSystem().getUri().toString());
    LOG.info("Exec output=\n{}", output);
  }

  private final static String UPLOAD_PREFIX = "test-upload-prefix";
  private final static String UPLOAD_NAME = "test-upload";

  @Test
  public void testUploads() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path path = path(UPLOAD_PREFIX + "/" + UPLOAD_NAME);

    describe("Cleaning up any leftover uploads from previous runs.");
    // 1. Make sure key doesn't already exist
    clearAnyUploads(fs, path);

    // 2. Confirm no uploads are listed via API
    assertNoUploadsAt(fs, path.getParent());

    // 3. Confirm no uploads are listed via CLI
    describe("Confirming CLI lists nothing.");
    assertNumUploads(path, 0);

    // 4. Create a upload part
    describe("Uploading single part.");
    createPartUpload(fs, fs.pathToKey(path), 128, 1);

    try {
      // 5. Confirm it exists via API..
      LambdaTestUtils.eventually(5000, /* 5 seconds until failure */
          1000, /* one second retry interval */
          () -> {
            assertEquals("Should be one upload", 1, countUploadsAt(fs, path));
          });

      // 6. Confirm part exists via CLI, direct path and parent path
      describe("Confirming CLI lists one part");
      LambdaTestUtils.eventually(5000, 1000,
          () -> { assertNumUploads(path, 1); });
      LambdaTestUtils.eventually(5000, 1000,
          () -> { assertNumUploads(path.getParent(), 1); });

      // 7. Use CLI to delete part, assert it worked
      describe("Deleting part via CLI");
      assertNumDeleted(fs, path, 1);

      // 8. Confirm deletion via API
      describe("Confirming deletion via API");
      assertEquals("Should be no uploads", 0, countUploadsAt(fs, path));

      // 9. Confirm no uploads are listed via CLI
      describe("Confirming CLI lists nothing.");
      assertNumUploads(path, 0);

    } catch (Throwable t) {
      // Clean up on intermediate failure
      clearAnyUploads(fs, path);
      throw t;
    }
  }

  @Test
  public void testUploadListByAge() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path path = path(UPLOAD_PREFIX + "/" + UPLOAD_NAME);

    describe("Cleaning up any leftover uploads from previous runs.");
    // 1. Make sure key doesn't already exist
    clearAnyUploads(fs, path);

    // 2. Create a upload part
    describe("Uploading single part.");
    createPartUpload(fs, fs.pathToKey(path), 128, 1);

    try {
      // 3. Confirm it exists via API.. may want to wrap with
      // LambdaTestUtils.eventually() ?
      LambdaTestUtils.eventually(5000, 1000,
          () -> {
            assertEquals("Should be one upload", 1, countUploadsAt(fs, path));
          });

      // 4. Confirm part does appear in listing with long age filter
      describe("Confirming CLI older age doesn't list");
      assertNumUploadsAge(path, 0, 600);

      // 5. Confirm part does not get deleted with long age filter
      describe("Confirming CLI older age doesn't delete");
      uploadCommandAssertCount(fs, ABORT_FORCE_OPTIONS, path, 0,
          600);

      // 6. Wait a second and then assert the part is in listing of things at
      // least a second old
      describe("Sleeping 1 second then confirming upload still there");
      Thread.sleep(1000);
      LambdaTestUtils.eventually(5000, 1000,
          () -> { assertNumUploadsAge(path, 1, 1); });

      // 7. Assert deletion works when age filter matches
      describe("Doing aged deletion");
      uploadCommandAssertCount(fs, ABORT_FORCE_OPTIONS, path, 1, 1);
      describe("Confirming age deletion happened");
      assertEquals("Should be no uploads", 0, countUploadsAt(fs, path));
    } catch (Throwable t) {
      // Clean up on intermediate failure
      clearAnyUploads(fs, path);
      throw t;
    }
  }

  @Test
  public void testUploadNegativeExpect() throws Throwable {
    runToFailure(E_BAD_STATE, Uploads.NAME, "-expect", "1",
        path("/we/are/almost/postive/this/doesnt/exist/fhfsadfoijew")
            .toString());
  }

  private void assertNumUploads(Path path, int numUploads) throws Exception {
    assertNumUploadsAge(path, numUploads, 0);
  }

  private void assertNumUploadsAge(Path path, int numUploads, int ageSeconds)
      throws Exception {
    if (ageSeconds > 0) {
      run(Uploads.NAME, "-expect", String.valueOf(numUploads), "-seconds",
          String.valueOf(ageSeconds), path.toString());
    } else {
      run(Uploads.NAME, "-expect", String.valueOf(numUploads), path.toString());
    }
  }

  private void assertNumDeleted(S3AFileSystem fs, Path path, int numDeleted)
      throws Exception {
    uploadCommandAssertCount(fs, ABORT_FORCE_OPTIONS, path,
        numDeleted, 0);
  }

  /**
   * Run uploads cli command and assert the reported count (listed or
   * deleted) matches.
   * @param fs  S3AFileSystem
   * @param options main command options
   * @param path path of part(s)
   * @param numUploads expected number of listed/deleted parts
   * @param ageSeconds optional seconds of age to specify to CLI, or zero to
   *                   search all parts
   * @throws Exception on failure
   */
  private void uploadCommandAssertCount(S3AFileSystem fs, String options[],
      Path path, int numUploads, int ageSeconds)
      throws Exception {
    List<String> allOptions = new ArrayList<>();
    List<String> output = new ArrayList<>();
    S3GuardTool.Uploads cmd = new S3GuardTool.Uploads(fs.getConf());
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    allOptions.add(cmd.getName());
    allOptions.addAll(Arrays.asList(options));
    if (ageSeconds > 0) {
      allOptions.add("-" + Uploads.SECONDS_FLAG);
      allOptions.add(String.valueOf(ageSeconds));
    }
    allOptions.add(path.toString());
    exec(cmd, buf, allOptions.toArray(new String[0]));

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(buf.toByteArray())))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] fields = line.split("\\s");
        if (fields.length == 4 && fields[0].equals(Uploads.TOTAL)) {
          int parsedUploads = Integer.valueOf(fields[1]);
          LOG.debug("Matched CLI output: {} {} {} {}", fields);
          assertEquals("Unexpected number of uploads", numUploads,
              parsedUploads);
          return;
        }
        LOG.debug("Not matched: {}", line);
        output.add(line);
      }
    }
    fail("Command output did not match: \n" + StringUtils.join("\n", output));
  }
}
