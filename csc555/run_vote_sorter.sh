time hadoop jar VoteCount.jar /user/ec2-user/input/combined /user/ec2-user/output/vote_count no "*"
time hadoop jar VoteSorter.jar /user/ec2-user/output/vote_count /user/ec2-user/output/vote_count_sorted no
