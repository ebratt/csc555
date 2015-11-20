#!/bin/bash
echo "#### STARTING GILDED BY SUBREDDIT - WITH COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_gilded_sorter_combiner_group_by_subreddit.sh >> run_remaining_jobs.log 2>&1;
echo "#### STARTGIN GILDED BY SUBREDDIT - NO COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_gilded_sorter_group_by_subreddit.sh >> run_remaining_jobs.log 2>&1;
echo "#### STARTING VOTES BY SUBREDDIT - WITH COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_vote_sorter_combiner_group_by_subreddit.sh >> run_remaining_jobs.log 2>&1;
echo "#### STARTING VOTES BY SUBREDDIT - NO COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_vote_sorter_group_by_subreddit.sh >> run_remaining_jobs.log 2>&1;
echo "#### STARTING COUNT OF ACRONYMS BY SUBREDDIT - WITH COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_wc_body_acronyms_combiner_group_by_subreddit.sh >> run_remaining_jobs.log 2>&1;
echo "#### STARTING COUNT OF ACRONYMS ALL - WITH COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_wc_body_acronyms_combiner.sh >> run_remaining_jobs.log 2>&1;
echo "#### STARTING COUNT OF ACRONYMS BY SUBREDDIT - NO COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_wc_body_acronyms_group_by_subreddit.sh >> run_remaining_jobs.log 2>&1;
echo "#### STARTING COUNT OF ACRONYMS ALL - NO COMBINER ####" >> run_remaining_jobs.log 2>&1;
./run_wc_body_acronyms.sh >> run_remaining_jobs.log 2>&1;
