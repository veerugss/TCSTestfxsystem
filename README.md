# Overview:
Assessment for the project FxEngine contains the spark scala solution for a Trading Engine  problem

Jars created:
- Jar: FxOrderEngine-1.0.0-SNAPSHOT.jar
- UberJar: FxOrderEngine-1.0.0-SNAPSHOT-jar-with-dependencies.jar

Command To Run Application Locally with uber jar (assuming your are inside repo):
```
spark-submit --name "FxOrderEngine" --master local --class veera.tcstest.spark.fxstockanalyzer.FxOrderEngine  target/FxOrderEngine-1.0.0-SNAPSHOT-jar-with-dependencies.jar data\order.csv
```

## Code walkthrough:
1. The main class is FxOrderEngine.scala
2 logger added to capture logs info
3. method loadOrderDF  defined to load data from source csv files
4. repartition applied to resuffle the data before processing
5. orderTypeFilter method defind to filter data based on BUY and SELL.
6. matchedOrderFilter method defined to join the buy and sell data and get the final result set.
7. print the data into log for testing and save them in data lake.

 Test Cases and Validation
1.FxOrderEngineTest  suite is implemanted to test functionality.
2.Test case FxOrderEngineTest Data File Loadind checek added
3.Test case Count by Buy,Count by Sell added to check the counts for buy and sell
  
