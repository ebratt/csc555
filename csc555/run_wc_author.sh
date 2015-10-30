time hadoop jar RCWordCount.jar \              # executable jar file
     /user/ec2-user/input/RC_2015-01 \         # input dir
     /user/ec2-user/output/wordcount_author \  # output dir
     author                                    # dimension
     no                                        # use combiner?

time hadoop jar RCTop10.jar \                      # executable jar file
     /user/ec2-user/output/wordcount_author        # input dir
     /user/ec2-user/output/wordcount_author_top10  # output dir

hadoop fs -cat /user/ec2-user/output/wordcount_author_top10/part-r-00000 | head -10  # get the first 10 rows in the output
