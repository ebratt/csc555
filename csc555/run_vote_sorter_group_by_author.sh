time hadoop jar VoteCount.jar /user/ec2-user/input/ /user/ec2-user/output/vote_count no author
time hadoop jar VoteSorter.jar /user/ec2-user/output/vote_count /user/ec2-user/output/vote_count_sorted no
