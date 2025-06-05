# Top-K Cities by Population using Hadoop MapReduce

This project implements a **Hadoop MapReduce** job that computes the **Top-K most populated cities** from a large CSV dataset. The implementation uses a custom `Writable` class and maintains a local `TreeMap` (min-heap-like behavior) to efficiently keep track of the top K entries.

## 📌 Features

- Parses a CSV file containing city population data
- Extracts city name and population
- Filters out invalid or empty entries
- Outputs the **K most populated cities** from the dataset

## 🧠 How It Works

- **Mapper**:
  - Skips the header
  - Extracts city names and population
  - Maintains a TreeMap of top K cities with highest populations locally

- **Combiner**:
  - Aggregates mapper outputs and filters top K again

- **Reducer**:
  - Merges all intermediate results and emits the final global Top K

## 📄 Input Format

The program expects a txt file like this:

id,city_name,state,country,population,...
1,New York,NY,USA,8419600,...
2,Los Angeles,CA,USA,3980400,...
3,Chicago,IL,USA,2716000,...

The population should be in the 5th column (index 4).

## ⚙️ Requirements

- Java 8+
- Apache Hadoop 2.x or 3.x
- (Optional) Maven for building

## 🏗️ Compilation

```bash
mvn clean package 
```

## 🚀 Usage

```bash
hadoop jar target/tp3-mapreduce-0.0.1.jar topk 10 /input/worldcitiespop.txt /output_k10
```
## 📦 Output

Output will contain the names and populations of the top K most populated cities:

New York	8419600
Los Angeles	3980400
Chicago	    2716000
...

