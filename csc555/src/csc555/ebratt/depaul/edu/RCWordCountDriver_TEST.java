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
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * RCTop10Driver_TEST is the test class for testing RCTop10Driver.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class RCWordCountDriver_TEST extends TestCase {
	
	private static Configuration conf = new Configuration();
	private static final String testName = "RCWordCountDriver_TEST";
	private static final Path localDir = new Path("./target/hdfs/" + testName);
	private static final Logger log = Logger.getLogger(RCWordCountDriver_TEST.class);
	
	MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
	MiniDFSCluster hdfsCluster = builder.build();
	String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort()} + "/";


	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		FileSystem localFS = FileSystem.getLocal(conf);
		log.info("About to delete directory: " + localDir.toString());
		boolean deleted = localFS.delete(localDir, true);
		log.info("Delete successful? " + deleted);
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		
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
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link csc555.ebratt.depaul.edu.RCWordCountDriver#run(java.lang.String[])}.
	 */
	@Test
	public final void testRun() {
		fail("Not yet implemented"); // TODO
	}

	/**
	 * Test method for {@link csc555.ebratt.depaul.edu.RCWordCountDriver#main(java.lang.String[])}.
	 */
	@Test
	public final void testMain() {
		fail("Not yet implemented"); // TODO
	}

}
