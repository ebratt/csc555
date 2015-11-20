time hadoop jar RCWordCount.jar /user/ec2-user/input/ /user/ec2-user/output/wordcount_subreddit subreddit yes "*"
time hadoop jar RCTop10.jar /user/ec2-user/output/wordcount_subreddit /user/ec2-user/output/wordcount_subreddit_top10
