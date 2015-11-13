package csc555.ebratt.depaul.edu;

/*
Copyright (c) 2015 Eric Bratt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

The Software shall be used for Good, not Evil.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * RCTop10Driver_TEST is the test class for testing RCTop10Driver.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class Test_RCWordCountDriver {

	private static Configuration CONF = new Configuration();
	private static final int DFS_REPLICATION_INTERVAL = 1;
	private static final Path TEST_ROOT_DIR_PATH = new Path(System.getProperty("test.build.data", "build/test/data"));
	private static final String TESTNAME = "Test_RCWordCountDriver";
	private static final File BASEDIR = new File("/Users/eric/git/csc555/csc555/build/test/target/hdfs/" + TESTNAME)
			.getAbsoluteFile();
	private static final Logger LOG = Logger.getLogger(Test_RCWordCountDriver.class);
	private static final int NUM_DATA_NODES = 1;

	static {
		CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
		CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
		CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, DFS_REPLICATION_INTERVAL);
		CONF.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, DFS_REPLICATION_INTERVAL);
	}

	private MiniDFSCluster cluster;
	private DistributedFileSystem fs;
	private FSNamesystem namesystem;
	private NameNode nn;

	private static Path getTestPath(String fileName) {
		return new Path(TEST_ROOT_DIR_PATH, fileName);
	}

	/** create a file with a length of <code>fileLen</code> */

	private void createFile(Path file, long fileLen, short replicas) throws IOException {
		Random random = new Random();
		DFSTestUtil.createFile(fs, file, fileLen, replicas, random.nextLong());
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// delete base dfs directory
		LOG.info("About to delete directory: " + BASEDIR.toString());
		boolean deleted = FileUtil.fullyDelete(BASEDIR);
		LOG.info("Delete successful? " + deleted);

		// set mini dfs cluster base directory
		LOG.info("mini dfs cluster directory: " + BASEDIR.getAbsolutePath());
		CONF.set("hdfs.minidfs.basedir", BASEDIR.getAbsolutePath());
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// create a new mini dfs cluster
		cluster = new MiniDFSCluster(CONF, NUM_DATA_NODES, true, null);
		String hdfsURI = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
		LOG.info("HDFS URI: " + hdfsURI);
		cluster.waitActive();
		namesystem = FSNamesystem.getFSNamesystem();
		fs = (DistributedFileSystem) cluster.getFileSystem();
		nn = cluster.getNameNode();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		LOG.info("Shutting down cluster...");
		cluster.shutdown();
	}

	/**
	 * Test method for
	 * {@link csc555.ebratt.depaul.edu.RCWordCountDriver#run(java.lang.String[])}
	 * .
	 */
	// @Test
	// public final void testRun() {
	// fail("Not yet implemented"); // TODO
	// }

	/**
	 * Test method for
	 * {@link csc555.ebratt.depaul.edu.RCWordCountDriver#main(java.lang.String[])}
	 * .
	 * 
	 * @throws Exception
	 */
	// @Test
	// public final void testMainWithBadArgs() throws Exception {
	// String[] args = {""};
	// RCWordCountDriver.main(args);
	// String actual = System.in.toString();
	// String expected = "Usage: RCWordCount.jar <in> <out> <aggregate>
	// <combiner? yes/no> <group by '*' for all>";
	// boolean condition = actual.equals(expected);
	// Assert.assertTrue(condition);
	// }

	/**
	 * Test that the build version is OK
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBuildVersion() throws IOException {
		assertNotNull("No namenode", nn);
		NamespaceInfo info = nn.versionRequest();
		assertEquals("Build version wrong", "1.1.2", info.getVersion());
	}

}
