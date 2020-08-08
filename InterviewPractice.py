"""
Given an array and an integer A, find the maximum for each contiguous subarray of size A.
Input: array = [1, 2, 3, 1, 4, 5, 2, 3, 6], A = 3
Output: 3 3 4 5 5 5 6
Below is a more detailed walkthrough of what you should be trying to code, using the example above:
subarray 1 = [1, 2, 3]
maximum of subarray 1 = 3
subarray 2 = [2, 3, 1]
maximum of subarray 2 = 3
subarray 3 = [3, 1, 4]
maximum of subarray 3 = 4
Etc.
"""

array = [1, 2, 3, 1, 4, 5, 2, 3, 6]
A = 3
for i in range(len(array)):
    subarray = array[i:i+A]
    if len(subarray) == 3:
        print(max(subarray))


"""
Suppose you're working for Reddit as an analyst. Reddit is trying to optimize its server allocation per subreddit, and you've been tasked with figuring out how much comment activity happens once a post is published.
Use your intuition to select a timeframe to query the data, as well as how you would want to present this information to the partnering team. The solution will be a SQL query with assumptions that you would need to state if this were asked in an interview. You have the attached tables.
Given the above, write a SQL query to highlight comment activity by subreddit. This problem is intended to test how you can think through vague/open-ended questions.

Table: posts
Column Name	    Data Type	Description
id	            integer	    id of the post
publisher_id	integer	    id the user posting
score	        integer	    score of the post
time	        integer	    post publish time in unix time
title	        string	    title of the post
deleted	        boolean	    is the post deleted?
dead	        boolean	    is the post active?
subreddit_id	integer	    id of the subreddit

Table: comments
Column Name	    Data Type	Description
id	            integer	    id of the comment
author_id	    integer	    id of the commenter
post_id	        integer	    id of the post the comment is nested under
parent_comment	integer	    id of parent comment that comment is nested under
deleted	        integer	    is comment deleted?

"""

"""
The server will need to withstand a high web traffic of commenters within a small timeframe after the post is created. We are not given comment timestamps, so I would start by looking at comments for posts no older than 24 hours from the current time. I would include deleted comments, as these still take up server resources when they are originally posted.

SELECT *, COUNT(comments.id) AS comment_count FROM posts
LEFT JOIN comments
    ON posts.id = comments.post_id
WHERE posts.time > NOW() - INTERVAL '24 HOUR'
    AND posts.deleted IS NULL
GROUP BY posts.id
"""
