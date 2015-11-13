time hadoop jar GildPercentPass1.jar /user/ec2-user/input/combined /user/ec2-user/output/gild_percent yes subreddit

time hadoop jar GildPercentPass2.jar /user/ec2-user/output/gild_percent /user/ec2-user/output/gild_percent_sorted yes

hadoop fs -cat /user/ec2-user/output/gild_percent_sorted/part-r-00000
