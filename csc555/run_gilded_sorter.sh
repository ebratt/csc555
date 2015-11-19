time hadoop jar GildedCount.jar /user/ec2-user/input/ /user/ec2-user/output/gilded_count no "*"
time hadoop jar GildedSorter.jar /user/ec2-user/output/gilded_count /user/ec2-user/output/gilded_count_sorted no
