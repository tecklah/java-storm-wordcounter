# java-storm-wordcounter
A sample project using Java to create and submit an Apache Storm topology to a local cluster. See package com.storm.sg.
The topology consists of 1 Kafka sprout and 3 bolts. 

Learning points :
1. There is a standard Kafka sprout available from Apache Storm.
2. It is possible push data from sprout to a specific bolt based on the stream name.

