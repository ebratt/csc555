# csc555
Final project home for DePaul University CSC555

Abstract: Three months ago, reddit user Stuck_In_the_Matrix submitted a post indicating he/she has made available a datasetii containing over 1.7 billion reddit comments in JSON format “…complete with comment, score, author, subreddit, position in comment tree and other fields that are available through reddit's API.” This project’s intention is to analyze this reddit data using Apache Hadoop (MapReduce and HDFS for algorithm execution, output for analysis, and data storage) and R (for correlation and plots). 

Motivation: At over 1 terabyte (uncompressed; 250 gigabytes compressed), the size of the dataset is a good use-case for a data mining project using Hadoop. Further, I am not a member of the reddit community and have never submitted a comment on the forum, so I thought analyzing reddit data would make for a good project topic as I have no biases toward the data. 

Tasks to Run:
The data comes in several compressed .bz2 files, some of which may be smaller than the block size increments in HDFS, so the first task is to use a MapReduce job to combine these into one large file before placing it on HDFS to maximize block usage. The combined data file should be > 1 terabyte.
MapReduce job to find the top 10 most active redditters 
MapReduce job to count the 10 most popular words (in total and by subreddit)
MapReduce job to count the 10 most popular acronyms (AFAIK, AMA, CCW, CMV, DAE, IMO, LOL, WIP, etc.)
MapReduce job to count the 10 most common n-grams (2, 3, 4)
Do some subreddits gild more than others?
Do some comments’ words elicit gildings more often than others?
Who is the top-gilded author?
What makes a comment controversial?
Do some subreddits vote more than others?
Is there some tendancy to upvote or downvote more in one subreddit than in another subreddit?
What is the average number of comments per top-level node in each subreddit?
The website claims the following with respect to how to choose a title for your reddit post: “Be careful though, if you're too aggressive it could backfire. Phrases like, "Spread the word!" or "AMAZING!" tend to annoy some redditors, who will make sure your post doesn't see the light of day.”
MapReduce job to output the average score of Titles that do not use the word, “AMAZING!” and the average score of Titles that use the word, “AMAZING!”
MapReduce job to output the length of Titles and their average scores (in total and by subreddit)
Use R to measure the correlation between length of Title and average score
MapReduce job to output length of Body and their average scores (in total and by subreddit) 
Use R to measure the correlation between length of Body and average score
MapReduce job to output the position of posts in their hierarchies and their average scores
Use R to measure correlation between the position of the post in the hierarchy and average score

Data to Use:
I will be using the data provided in the torrent link listed below (endnote ii). It contains over 1.7 billion reddit comments.
The plan is to download the files using uTorrent to a local machine and then scp these files to an AWS server. I am choosing this route (versus a direct torrent on the AWS server) because I’m not clear on Amazon’s restrictions/costs related to streaming torrents on their machines.

Time-Permitting:
Compare run-time differences with Hadoop configuration changes (add mappers/reducers, adding a combiner, setting replication/compression, adding new nodes)
Implement MapReduce jobs with Hive, Pig, and YSmart and compare their run-time performance.

Background: 
According to the help page of the websitei,
Reddit is a source for what's new and popular on the Internet.
Reddit is made up of many individual communities, also known as "subreddits". Each community has its own page, subject matter, users and moderators
Users post stories, links, and media to these communities, and other users vote and comment on the posts.
Through voting, users determine what posts rise to the top of community pages and, by extension, the public home page of the site.
Anyone with an account can post on reddit. 
A post's score is simply the number of upvotes minus the number of downvotes. If five users like the submission and three users don't, it will have a score of 2.

Sample data entry:
{"parent_id":"t3_5yba3","created_utc":"1192450635","ups":1,"controversiality":0,"distinguished":null,"subreddit_id":"t5_6","id":"c0299an","downs":0,"archived":true,"link_id":"t3_5yba3","score":1,"author":"bostich","score_hidden":false,"body":"test","gilded":0,"author_flair_text":null,"subreddit":"reddit.com","edited":false,"author_flair_css_class":null,"name":"t1_c0299an","retrieved_on":1427426409}

Detailed Steps:
1. Get the data
a. Downloaded a torrent client called uTorrent
b. Spent roughly 6 hours downloading the file from 40 different seed peers at magnet:?xt=urn:btih:7690f71ea949b868080401c749e878f98de34d3d&dn=reddit%5Fdata&tr=http%3A%2F%2Ftracker.pushshift.io%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80
c. Files came in directory structure as follows:
i. reddit_data 
1. 2007
a. RC_2007-10.bz2 (13.3 MB)
b. RC_2007-11.bz2 (28.7 MB)
c. RC_2007-12.bz2 (31.2 MB)
2. 2008
a. 
3. 2009
4. 2010
5. 2011
6. 2012
7. 2013
8. 2014
9. 2015
10. README
d. According to the README file, “RC” represents “reddit comments.” The remainder of the filename represents the year (yyyy) followed by the month (mm) of the created date.
2. Validated the checksums