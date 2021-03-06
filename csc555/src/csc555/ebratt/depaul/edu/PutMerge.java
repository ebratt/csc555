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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * PutMerge is a class that takes multiple files on a local filesystem and puts
 * them on HDFS in a merged file.
 * 
 * @author Eric Bratt
 * @version 11/11/2015
 * @since 11/11/2015
 * 
 */
public class PutMerge {

	/**
	 * @param args
	 *            [0] the input directory on the local filesystem
	 * @param args
	 *            [1] the fully-qualified output filename on HDFS
	 * @throws IOException
	 *             in the even there is an issue with input/output
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 3) {
			System.err.println("Usage: PutMerge.jar <fs.default.name> <in> <out>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		conf.set("fs.default.name", args[0]);

		FileSystem hdfs = FileSystem.get(conf);
		FileSystem localFS = FileSystem.getLocal(conf);

		Path localDir = new Path(args[1]);
		Path hdfsFile = new Path(args[2]);

		try {
			if (hdfs.exists(hdfsFile)) {
				System.out.println("deleting target file: "
						+ hdfsFile.toString());
				hdfs.delete(hdfsFile, true);
			}
			System.out.println("copying/merging files from: local:/"
					+ localDir.toString() + " to hdfs:/" + hdfsFile.toString());
			FileUtil.copyMerge(localFS, localDir, hdfs, hdfsFile, false, conf,
					null);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
