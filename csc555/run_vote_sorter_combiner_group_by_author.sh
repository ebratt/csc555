time hadoop jar VoteCount.jar /user/ec2-user/input/ /user/ec2-user/output/vote_count yes author
time hadoop jar VoteSorter.jar /user/ec2-user/output/vote_count /user/ec2-user/output/vote_count_sorted yes
