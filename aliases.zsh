alias testQuery='hdfs dfs -rm -f -r /user/cs4433/project3/output/;rm -f -r ~/shared_folder/output/output;spRun ./1-Queries/query1.py ; hdfs dfs -get /user/cs4433/project3/output/ ~/shared_folder/output/'
alias changeInput='hdfs dfs -rm -f -r /user/cs4433/project3/input/;hdfs dfs -put ~/shared_folder/input/ /user/cs4433/project3/input'
