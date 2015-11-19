time hadoop jar AverageScore.jar /user/ec2-user/input/ /user/ec2-user/output/average_score no author
time hadoop jar AverageScoreRanker.jar /user/ec2-user/output/average_score /user/ec2-user/output/average_score_ranked no
