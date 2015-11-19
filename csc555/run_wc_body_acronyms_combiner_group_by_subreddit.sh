time hadoop jar RCWordCountAcronyms.jar /user/ec2-user/input/ /user/ec2-user/output/wordcount_body_acronyms body yes subreddit

time hadoop jar RCTop10.jar /user/ec2-user/output/wordcount_body_acronyms /user/ec2-user/output/wordcount_body_acronyms_top10

hadoop fs -cat /user/ec2-user/output/wordcount_body_acronyms_top10/part-r-00000 | head -10  # get the first 10 rows in the output
