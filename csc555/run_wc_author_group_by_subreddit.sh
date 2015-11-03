time hadoop jar RCWordCount.jar /user/ec2-user/input/combined /user/ec2-user/output/wordcount_author author no subreddit

time hadoop jar RCTop10.jar /user/ec2-user/output/wordcount_author /user/ec2-user/output/wordcount_author_top10

hadoop fs -cat /user/ec2-user/output/wordcount_author_top10/part-r-00000 | head -10  # get the first 10 rows in the output
