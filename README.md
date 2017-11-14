# Analyzing-ACM-Citation-Network
Implement weighted Page Rank Algorithm to find most influential paper in ACM library. •MapReduce:Java •Spark:Scala 

The project is organized in following files

1) Link.txt: It is the output to the first part of the project. The file produces a tab separeted list of paper1 paper2 where paper1 cites paper2 generated from MapReduce Code.

2) graph.jpg: It is the indegree ditribution graph where X-axis represents indegree k and Y-axis represents indegree distribution (k).
 ![graph](https://user-images.githubusercontent.com/25961531/32805154-44caf8ce-c94e-11e7-9cfa-5f0ae89d7e4c.jpg)
3) script-step 2.3: This file contains the script for computing average clustering coefficient and average path length for random graphs as well as for the graph generated from previous processing

4) Source files: This folder contains the source code for part 1,2 and 3 of the project. Part 1 folder contains the MapReduce code for extracting citations betwee two papers. Further, PaperGraph.scala contains the code for indegree distribution whereas pageRank.scala has the code for page rank implementation.

5) Report: This document contains detailed information on the workflow of the project.

6) Presentation: It contains a power point presentation briefly explaining project workflow.
