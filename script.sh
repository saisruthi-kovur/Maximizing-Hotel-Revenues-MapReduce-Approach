python DBMS_DataPreprocessing.py $1 $2
hadoop fs -mkdir /DBMSShasru/
hadoop fs -mkdir /DBMSShasru/Input
hadoop fs -mkdir /DBMSShasru/Input2
hadoop fs -mkdir /DBMSShasru/Input3
hadoop fs -put Intermediate_input/input.csv /DBMSShasru/Input
hadoop fs -put Intermediate_input/input_season.csv /DBMSShasru/Input2
hadoop fs -put Intermediate_input/input_country.csv /DBMSShasru/Input3
hadoop jar ProfitableMonth/jarFiles/test1.jar  ProfitableMonthSorted /DBMSShasru/Input /DBMSShasru/Output
hadoop jar ProfitableMonth/jarFiles/test2.jar  ProfitableMonthSeason /DBMSShasru/Input2 /DBMSShasru/Output2
hadoop jar ProfitableMonth/jarFiles/test3.jar  ProfitableMonthCountry /DBMSShasru/Input3 /DBMSShasru/Output3
hadoop fs -copyToLocal /DBMSShasru/Output/part-r-00000 $3/Output1.txt
hadoop fs -copyToLocal /DBMSShasru/Output2/part-r-00000 $3/Output2.txt
hadoop fs -copyToLocal /DBMSShasru/Output3/part-r-00000 $3/Output3.txt
