Name:       FRANCIS JAMES
ID:         2247686795
Assignment: HW1
================================================================================
Main Folder
--------------------------------------------------------------------------------
Francis_James_HW1
├── python
│   ├── Francis_James_result_task1.txt
│   ├── Francis_James_result_task2.txt
│   ├── Francis_James_task1.py
│   └── Francis_James_task2.py
├── README.txt
└── scala
    ├── Francis_James_result_task1.txt
    ├── Francis_James_result_task2.txt
    ├── Francis_James_task1.jar
    ├── Francis_James_task1.scala
    ├── Francis_James_task2.jar
    └── Francis_James_task2.scala
================================================================================
PYTHON
--------------------------------------------------------------------------------
Python files can be run as follows:
    1) locate spark-submit if not in path
    2) use spark-submit passing python scripts from python folder and arguments to those scripts

    $ YOUR_SPARK_HOME/bin/spark-submit ./Francis_James_HW1/python/Francis_James_task1.py /path/to/users.dat /path/to/ratings.dat
    $ YOUR_SPARK_HOME/bin/spark-submit ./Francis_James_HW1/python/Francis_James_task2.py /path/to/users.dat /path/to/ratings.dat /path/to/movies.dat

    - This will produce two files in your current working directory:
        Francis_James_result_task1.txt and Francis_James_result_task2.txt respectively.

    **NOTE** If you execute this script from within Francis_James_HW1/python, i.e. cwd() == /path/to/Francis_James_HW1/python/
    it will replace the current result files.
================================================================================
SCALA
--------------------------------------------------------------------------------
Scala files can be run as follows:
    1) locate spark-submit if not in path
    2) use spark-submit indicating the class, master, as well as the jar file and arguments

    $ YOUR_SPARK_HOME/bin/spark-submit --class "Task1" --master "local[1]" ./Francis_James_HW1/scala/Francis_James_task1.jar /path/to/users.dat /path/to/ratings.dat
    $ YOUR_SPARK_HOME/bin/spark-submit --class "Task2" --master "local[1]" ./Francis_James_HW1/scala/Francis_James_task2.jar /path/to/users.dat /path/to/ratings.dat /path/to/movies.dat

    - This will produce two files in your current working directory:
        Francis_James_result_task1.txt and Francis_James_result_task2.txt respectively.

    **NOTE** If you execute this script from within Francis_James_HW1/scala, i.e. cwd() == /path/to/Francis_James_HW1/scala/
    it will replace the current result files.
================================================================================
