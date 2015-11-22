time hadoop jar ../RCWordCount.jar /user/ec2-user/input/ /user/ec2-user/output/wordcount_body body yes "*"
time hadoop jar ../RCTop10.jar /user/ec2-user/output/wordcount_body /user/ec2-user/output/wordcount_body_top10
