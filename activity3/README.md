# 🎲 Pi Estimation using Monte Carlo in Hadoop MapReduce

This project implements a **Hadoop MapReduce** job to estimate the value of **π (pi)** using the **Monte Carlo method**. It demonstrates how to build a custom `InputFormat` to generate data on-the-fly and compute an approximation of π in parallel.

---

## 📌 Features

- ✅ Custom `Writable` class to represent 2D points (`Point2DWritable`)
- ✅ Custom `InputFormat` (`RandomPointInputFormat`) to generate random points
- ✅ Virtual `InputSplit` (`FakeInputSplit`)
- ✅ Random point generator (`RandomPointRecordReader`)
- ✅ Flexible configuration:
  - Number of mappers (splits)
  - Number of points per mapper
- ✅ Calculates and outputs an approximation of π using MapReduce

---

## 🧠 Method: Monte Carlo Simulation

The Monte Carlo method is a probabilistic technique based on random sampling.

We simulate:
- Random points in a unit square (0 ≤ x, y ≤ 1)
- A quarter-circle of radius 1 inside the square

We estimate π with the formula:

\[
\pi \approx 4 \times \frac{\text{points inside the circle}}{\text{total points}}
\]

Where a point \((x, y)\) is **inside** the circle if:

\[
x^2 + y^2 \leq 1
\]

---

## 📁 Project Structure

- `Point2DWritable`: A custom Hadoop `Writable` to handle 2D points
- `RandomPointInputFormat`: Custom input format to generate data
- `FakeInputSplit`: Represents virtual input blocks
- `RandomPointRecordReader`: Random point generator per split
- `PiMapper`: Checks whether points fall inside the quarter-circle
- `PiReducer`: Aggregates results and prepares final output
- `PiEstimation`: Main driver class to run the MapReduce job

---

## ⚙️ Requirements

- Java 8+
- Apache Hadoop 2.x or 3.x
- (Optional) Maven or Gradle for building JARs

---

## 🏗️ Build Instructions

### Compile manually:

```bash
mvn clean package
```

## 🚀 How to Run

```bash
hadoop jar target/tp3-mapreduce-0.0.1.jar bigdata.TP3 4 100 /user/tarek/output
```