time hadoop jar GildPercentPass1.jar /user/ec2-user/input/ /user/ec2-user/output/gild_percent no subreddit

time hadoop jar GildPercentPass2.jar /user/ec2-user/output/gild_percent /user/ec2-user/output/gild_percent_sorted no

hadoop fs -cat /user/ec2-user/output/gild_percent_sorted/part-r-00000
