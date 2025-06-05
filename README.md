# 📊 Hadoop MapReduce Projects

This repository contains a collection of Hadoop MapReduce projects developed to explore and demonstrate the power of distributed computing using Apache Hadoop.

These projects include:
- 🔝 **Top-K Cities by Population**
- 🎲 **π Estimation using Monte Carlo Simulation**

Each project showcases different concepts of the MapReduce paradigm, including custom input formats, Writables, combiners, reducers, and sorting mechanisms.

---

## 📁 Project Overview

### 1️⃣ Top-K Cities by Population

Estimate the **Top-K most populated cities** using a distributed approach.

#### 🔧 Features:
- Custom `Writable` for city/population
- In-mapper local top-K aggregation using `TreeMap` (Min-Heap simulation)
- Combiner to reduce shuffle size
- Reducer computes the final global top-K


### 2️⃣ Pi Estimation using Monte Carlo

Estimate **π (pi)** using the Monte Carlo method implemented with **Hadoop MapReduce**.

---

#### 🧠 Method

Approximate π using random (x, y) points in a unit square, counting how many fall within a quarter circle.

\[
\pi \approx 4 \times \frac{\text{points inside the circle}}{\text{total points}}
\]

A point \((x, y)\) is considered **inside** the quarter circle if:

\[
x^2 + y^2 \leq 1
\]

---

#### 🔧 Features

- ✅ Custom `Writable` class for 2D points
- ✅ Fully virtual `InputFormat` generating data at runtime
- ✅ Virtual `InputSplit` (FakeInputSplit)
- ✅ Random point generator per mapper
- ✅ Configurable number of mappers and points per mapper