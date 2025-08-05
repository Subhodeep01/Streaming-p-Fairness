# Streaming Fairness Framework

## 🛠 How to Get Started

### 1. Clone the Repository

```bash
git clone <repo_url>
```

> 💡 It is highly recommended to open the directory in **VSCode**. The code was built on Windows 11 OS. Should be compatible with MacOS but Windows OS is recommended.

### 2. Set Up Python Environment

Create a Python 3.9+ virtual environment and install dependencies:

```bash
pip install confluent-kafka pandas numpy matplotlib more_itertools
```

### 3. Start Kafka via Docker

Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop) is installed and running, then run:

```bash
docker compose -f ./zk-single-kafka-single.yml up -d
```

### 4. Prepare Directory Structure

Before producing and consuming data:

- Delete and rereate the `datasets` directory and place all datasets there.  
  📥 Download from: [Datasets](https://drive.google.com/drive/folders/1HZG-87E68jxIp5kVM9nrMapfxOJPcyO0?usp=sharing)
- Create a `metrics` directory to store performance metrics.

---

## ▶️ Run Producer

```bash
python run_producer.py
```

> 🛠 If errors occur (e.g. missing modules), use:
```bash
python producer.py --topic_name <your_topic>
```

### ⚠️ Input Notes

- You will be prompted for a topic name—change it when switching datasets or attributes.
- When prompted for `is_discrete`, input `1` (Yes) or `0` (No).
- ❗ If you make a mistake during input, terminate and rerun with a new topic.

---

## 📖 Run Read-Only Consumer

```bash
python run_consumer_readable.py
```

> 🛠 If errors occur, use:
```bash
python consumer.py --window_size <size> --block_size <size> --topic_name <topic> --max_windows <count>
```

### ⚠️ Input Notes

- ✅ Use the same topic name as the producer.
- ✅ Ensure `block size` divides `window size`.
- ✅ For fairness counts, input **absolute values** (e.g., 10 for M, 10 for F in a block of size 20).
- ❗ The counts **must sum to block size** and **none should be zero**, or the program will crash.
- 📈 Results will be printed and performance metrics for the session will be stored in `metrics/`.

---

## ✨ Run Editable/Reorderable Consumer

```bash
python run_consumer_editable.py
```

> 🛠 If errors occur, use:
```bash
python consumer_editable_performance.py --window_size <size> --block_size <size> --topic_name <topic> --max_windows <count> --landmark <value> --brt_force False --backward False
```

### ⚠️ Input Notes

- ✅ Use the same topic name as the producer.
- ✅ `block size` must divide `window size + landmark`.
- ✅ `landmark` must not exceed `window size`.
- ✅ Use **counts** for fairness (e.g., 10 for F and 10 for M).
- ❗ Counts **must sum to block size** and **none should be zero**.
- 📈 Results will be printed and performance metrics for the session will be stored in `metrics/`.

---

## 📄 Technical Report

The tech report is included as a PDF in [Technical_Report.pdf](Technical_Report.pdf).