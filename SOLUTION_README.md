
| Project           | Created   | Updated    | Version |
|-------------------|-----------|------------|---------|
| DE Technical Test | 16/6/2017 | 19/6/2017  | 0.2     |

**UPDATE THE APP CONFIG BEFORE RUNNING / BUILDING THE THE FAT JAR**

# Environment

*   OS Version => MacOS 10.12.5
*   Spark Version => "2.1.0"
*   scalaVersion => “2.11.7”

# Creating the Spark Jobs

Create the Spark Fat Jar with ```sbt assembly``` outputs **SparkFatJar.jar**

### Part A

```spark-submit --master local[*] --class "DDTechnical.PartA" target/scala-2.11/SparkFatJar.jar```

### Part B

```spark-submit --master local[*] --class "DDTechnical.PartB" target/scala-2.11/SparkFatJar.jar```

### Part C

```spark-submit --master local[*] --class "DDTechnical.PartC" target/scala-2.11/SparkFatJar.jar```

# Unit Testing

Run the following command to execute the tests:

```
	sbt test
```