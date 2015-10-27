# CSC555, DePaul University
**Abstract:** Three months ago, reddit user `Stuck_In_the_Matrix` submitted a post (https://redd.it/3bxlg7) indicating he/she has made available a data set containing over 1.7 billion reddit comments in JSON format “…complete with comment, score, author, subreddit, position in comment tree and other fields that are available through reddit's API.” This project’s intention is to analyze this reddit data using Apache Hadoop (MapReduce and HDFS for algorithm execution, output for analysis, and data storage) and R (for correlation and plots). 

**Motivation:** At over 1 terabyte (uncompressed; 250 gigabytes compressed), the size of the data set is a good use-case for a data mining project using Hadoop. Further, I am not a member of the reddit community and have never submitted a comment on the forum, so I thought analyzing reddit data would make for a good project topic as I have no biases toward the data. 

**Tasks to Run/Analysis:**

 - The data comes in several compressed .bz2 files, some of which may be smaller than the block size increments in HDFS, so the first task is to use a MapReduce job to combine these into one large file before placing it on HDFS to maximize block usage. The combined data file should be > 1 terabyte.
 - MapReduce job to find the top 10 most active redditters
 - MapReduce job to count the 10 most popular words (in total and by subreddit)
 - MapReduce job to count the 10 most popular acronyms (AFAIK, AMA, CCW, CMV, DAE, IMO, LOL, WIP, etc.)
 - MapReduce job to count the 10 most common n-grams (2, 3, 4)
 - Do some subreddits gild more than others?
 - Do some comments’ words elicit gildings more often than others?
 - Who is the top-gilded author?
 - What makes a comment controversial?
 - Do some subreddits vote more than others?
 - Is there some tendancy to upvote or downvote more in one subreddit than in another subreddit?
 - What is the average number of comments per top-level node in each subreddit?
 - The website claims the following with respect to how to choose a title for your reddit post: 
 

> Be careful though, if you're too aggressive it could backfire. Phrases like, "Spread the word!" or "AMAZING!" tend to annoy some redditors, who will make sure your post doesn't see the light of day.

 - MapReduce job to output the average score of Titles that do not use the word, “AMAZING!” and the average score of Titles that use the word, “AMAZING!”
 - MapReduce job to output the length of Titles and their average scores (in total and by subreddit)
 - Use R to measure the correlation between length of Title and average score
 - MapReduce job to output length of Body and their average scores (in total and by subreddit)
 - Use R to measure the correlation between length of Body and average score
 - MapReduce job to output the position of posts in their hierarchies and their average scores
 - Use R to measure correlation between the position of the post in the hierarchy and average score

**Data to Use:**
I am using the data provided in this torrent link: 

    magnet:?xt=urn:btih:7690f71ea949b868080401c749e878f98de34d3d&dn=reddit%5Fdata&tr=http%3A%2F%2Ftracker.pushshift.io%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80

It contains over 1.7 billion reddit comments. I used uTorrent tow download to my local machine and then uploaded these files to an AWS S3 bucket called 'ebratt.depaul.edu'. For example, to download the October, 2007 Reddit Comment data in `.bz2` format:

    wget https://s3-us-west-2.amazonaws.com/ebratt.depaul.edu/RC_2007-10.bz2

**Time-Permitting:**

 1. Compare run-time differences with Hadoop configuration changes (add mappers/reducers, adding a combiner, setting replication/compression, adding new nodes)
 2. Implement MapReduce jobs with Hive, Pig, and YSmart and compare their run-time performance.

**Background:** 
According to the help page of the website: 

 3. Reddit is a source for what's new and popular on the Internet.
 4. Reddit is made up of many individual communities, also known as "subreddits".
 5. Each community has its own page, subject matter, users and moderators.
 6. Users post stories, links, and media to these communities, and other users vote and comment on the posts. Through voting, users determine what posts rise to the top of community pages and, by extension, the public home page of the site.
 7. Anyone with an account can post on reddit. 
 8. A post's score is simply the number of upvotes minus the number of downvotes. If five users like the submission and three users don't, it will have a score of 2.

**Sample data entry:**

> `{"parent_id":"t3_5yba3","created_utc":"1192450635","ups":1,"controversiality":0,"distinguished":null,"subreddit_id":"t5_6","id":"c0299an","downs":0,"archived":true,"link_id":"t3_5yba3","score":1,"author":"bostich","score_hidden":false,"body":"test","gilded":0,"author_flair_text":null,"subreddit":"reddit.com","edited":false,"author_flair_css_class":null,"name":"t1_c0299an","retrieved_on":1427426409}`

**Progress Report:**

 - Installed Eclipse 3.8.0 so that I could use/test the HDT plugin (http://hdt.incubator.apache.org/) to develop and test locally on my linux desktop. 
 - Downloaded and installed HDT on my linux desktop
 - Downloaded, installed, and configured Hadoop 1.1.2 on my linux desktop (1.1 and 2.2 are the only versions of Hadoop that are currently supported with the HDT plugin). 
 - Created a `MergePut` class to merge the 92 Reddit Comment files into a single file on HDFS. With a default 64Mb block-size, that should produce roughly 15,625 blocks.
 - Created an `RCWordCount_Author` class to count the number of comments by author
 - Created an `RCWordCount_Top10` class to sort some file on HDFS and output the top 10 in descending order
