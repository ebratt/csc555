time hadoop jar AverageScore.jar /user/ec2-user/input/combined /user/ec2-user/output/average_score yes author
time hadoop jar AverageScoreRanker.jar /user/ec2-user/output/average_score /user/ec2-user/output/average_score_ranked yes
