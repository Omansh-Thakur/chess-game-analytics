# Chess Game Analytics using Hadoop and Spark

**Project Report | CSBB 422 – Big Data Analytics**

**Group members**

| Member | Roll Number | Role |
|--------|-------------|------|
| Sanchit Agrawal | 231210090 | A – Cluster setup |
| Omansh Thakur | 231210080 | B – Data parsing and HDFS |
| Saumya Prakash Narayan | 231210093 | C – Spark jobs |
| Pankaj Jaiswal | 231210083 | D – Dashboard and documentation |

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Problem Statement and Objectives](#2-problem-statement-and-objectives)
3. [Dataset](#3-dataset)
4. [System Architecture](#4-system-architecture)
5. [Technology Stack](#5-technology-stack)
6. [Cluster Setup and Configuration](#6-cluster-setup-and-configuration)
7. [Implementation](#7-implementation)
8. [Results and Discussion](#8-results-and-discussion)
9. [Validation and Performance Evaluation](#9-validation-and-performance-evaluation)
10. [Basic Commands Used](#10-basic-commands-used)
11. [Design Decisions](#11-design-decisions)
12. [Challenges and Solutions](#12-challenges-and-solutions)
13. [Limitations and Future Work](#13-limitations-and-future-work)
14. [Conclusion](#14-conclusion)
15. [Team Contributions](#15-team-contributions)
16. [Reproducing the Project](#16-reproducing-the-project)
17. [References](#17-references)

---

## Abstract

This project builds an end-to-end big data analytics system on a real multi-node cluster. Online chess games from the open Lichess database are parsed, stored in the Hadoop Distributed File System (HDFS), and analysed with Apache Spark jobs running on YARN across one master node and two worker nodes. Ten analyses cover opening performance by rating level, game length, colour advantage, rating upsets, how games end, activity patterns, and the relationship between time pressure and blunders. The results are exported as tables and presented in a Streamlit web dashboard. The project demonstrates installation and configuration of Hadoop and Spark, use of their command-line tools, and a working data pipeline from raw files to an interactive user interface.

---

## 1. Introduction

Online chess platforms record every game played, producing very large, structured and publicly available datasets. A single month of games from Lichess contains millions of records and can exceed 30 GB even when compressed, which makes it a natural candidate for distributed storage and parallel processing rather than single-machine tools.

This project applies the Hadoop ecosystem to that data. HDFS provides replicated, block-based storage, YARN allocates cluster resources, and Spark performs the analysis in parallel. A web dashboard makes the findings accessible to a non-technical reader.

## 2. Problem Statement and Objectives

**Problem statement.** Analyse millions of online chess games from the [Lichess open database](https://database.lichess.org) (CC0 licence) to discover patterns in openings, game length, colour advantage, rating upsets and time-pressure blunders, using a distributed big data system.

**Objectives**

1. Install and configure Hadoop (HDFS and YARN) and Spark on a cluster of one master and two worker nodes.
2. Build a pipeline that parses raw chess game files, stores them in HDFS, and processes them with Spark on the cluster.
3. Produce ten analyses that answer concrete questions about chess games.
4. Present the results in a web-based user interface.
5. Execute and explain the basic usage commands of the tools used.
6. Validate the results and document the system in a public GitHub repository.

## 3. Dataset

| Property | Value |
|----------|-------|
| Source | Lichess open database, https://database.lichess.org (standard rated games) |
| Licence | CC0 (public domain) |
| File analysed | `lichess_db_standard_rated_2026-10.pgn.zst` |
| Compressed size | **31 GB** |
| Number of games | approximately 4.5 million |
| Format | PGN (Portable Game Notation), compressed with Zstandard |

The full October file contains far more games than the three virtual machines (4 GB RAM each) and the single-node parser could process in the available time, so a subset of approximately 4.5 million games was analysed.

A PGN game consists of header tags followed by one line of moves. Newer games also carry the remaining clock time and, for a subset of games, the engine evaluation after each move. An illustrative game record looks like this:

```
[Event "Rated Blitz game"]
[Site "https://lichess.org/abc12345"]
[Result "1-0"]
[UTCDate "2016.01.05"]
[UTCTime "10:00:01"]
[WhiteElo "1500"]
[BlackElo "1480"]
[ECO "B90"]
[Opening "Sicilian Defense: Najdorf Variation"]
[TimeControl "300+0"]
[Termination "Normal"]

1. e4 { [%eval 0.17] [%clk 0:05:00] } 1... c5 { [%eval 0.20] [%clk 0:05:00] } 2. Nf3 ...
```

The raw file is text that is not directly usable by Spark, so it is converted to structured tables by the parser described in Section 7.1.

## 4. System Architecture

### 4.1 Data flow

```
Lichess PGN (.pgn.zst)
  → parse_pgn.py (on master)     → games.csv + moves.csv
  → HDFS /chess/raw               (blocks replicated across worker1 and worker2)
  → Spark on YARN: etl.py         → Parquet in /chess/parquet
  → Spark on YARN: analysis.py    → result CSVs in /chess/results
  → hdfs getmerge                 → results/*.csv in the repo
  → Streamlit dashboard           → http://master:8501
```

### 4.2 Cluster layout

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    master    │     │   worker1    │     │   worker2    │
│              │     │              │     │              │
│  NameNode    │     │  DataNode    │     │  DataNode    │
│  ResourceMgr │     │  NodeManager │     │  NodeManager │
│  Spark Client│     │              │     │              │
│  Dashboard   │     │              │     │              │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────── Same LAN (bridged VMs) ───────┘
```

### 4.3 Role of each component

| Component | Role in this project |
|-----------|----------------------|
| NameNode (master) | Keeps HDFS metadata: which blocks make up each file and where they are stored |
| DataNodes (workers) | Store the data blocks; each block is kept on both workers (replication factor 2) |
| ResourceManager (master) | Accepts applications and grants containers (CPU and memory) on the workers |
| NodeManagers (workers) | Launch and monitor the containers in which Spark executors run |
| Spark driver (master) | Plans the job as stages and tasks and coordinates the executors |
| Spark executors (workers) | Run the tasks in parallel on the data blocks stored on the workers |
| Spark History Server | Keeps the record of finished jobs for inspection |
| Streamlit app (master) | Reads the exported result tables and renders the dashboard |

## 5. Technology Stack

| Component | Version | Installed on | Purpose |
|-----------|---------|--------------|---------|
| Ubuntu Server | 22.04 LTS | all nodes (VirtualBox VMs) | Node operating system |
| OpenJDK | 11 | all nodes | Runtime for Hadoop and Spark |
| Hadoop (HDFS and YARN) | 3.3.6 | all nodes | Distributed storage and resource management |
| Apache Spark | 3.5.3 | master only (runs on YARN) | Distributed processing (PySpark) |
| Python | 3.10 | all nodes (same version) | Parser, Spark jobs, dashboard |
| Streamlit, pandas, plotly | latest | master only | Web user interface and charts |
| Git and GitHub | – | all members | Version control and submission |

## 6. Cluster Setup and Configuration

### 6.1 Nodes

The cluster has one master and two worker nodes, each running on a separate physical PC as an Ubuntu Server 22.04 virtual machine in Oracle VirtualBox with a bridged network adapter, so that every node has its own address on the same local network.

| Node | Role | Host PC | VM resources |
|------|------|---------|--------------|
| master | NameNode, ResourceManager, Spark client, dashboard | PC 1 | **4GB RAM, 2 vCPUs** |
| worker1 | DataNode, NodeManager | PC 2 | **4GB RAM, 2 vCPUs** |
| worker2 | DataNode, NodeManager | PC 3 | **4GB RAM, 2 vCPUs** |

### 6.2 Key configuration

The complete configuration files are stored in the [`cluster-setup/`](cluster-setup/) folder.

| Setting | Value | File |
|---------|-------|------|
| Default file system | `hdfs://master:9000` | core-site.xml |
| HDFS replication factor | 2 | hdfs-site.xml |
| MapReduce framework | YARN | mapred-site.xml |
| YARN ResourceManager host | master | yarn-site.xml |
| YARN NodeManager memory and vcores | sized to each VM's RAM and CPUs (exact values in the file) | yarn-site.xml |
| YARN virtual-memory check | disabled | yarn-site.xml |
| Worker nodes | worker1, worker2 | workers |
| Spark master | yarn (client mode) | spark-defaults.conf |
| Spark executors | 2 executors, 1 core and 1 GB each | spark-defaults.conf |
| Shuffle partitions | 16 | spark-defaults.conf |
| Spark jar archive | `hdfs://master:9000/spark-jars/spark-libs.jar` | spark-defaults.conf |
| Event logging | enabled, directory `hdfs://master:9000/spark-logs` | spark-defaults.conf |

### 6.3 Installation summary

**All nodes**

```bash
sudo hostnamectl set-hostname master   # or worker1 / worker2
# Edit /etc/hosts on ALL nodes:
#   192.168.1.10 master
#   192.168.1.11 worker1
#   192.168.1.12 worker2

sudo apt update && sudo apt install -y openjdk-11-jdk openssh-server python3 python3-pip zstd
sudo ufw disable
```

Passwordless SSH was set up from the master to all nodes, and a common user named `hadoop` was created on every node.

**Hadoop (master, then copied to workers)**

```bash
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz && mv hadoop-3.3.6 hadoop
# Edit config files (see cluster-setup/), then copy to workers:
scp -r ~/hadoop worker1:~/
scp -r ~/hadoop worker2:~/
# Format the NameNode ONCE, then start:
hdfs namenode -format
start-dfs.sh && start-yarn.sh
```

**Spark (master only)**

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar -xzf spark-3.5.3-bin-hadoop3.tgz && mv spark-3.5.3-bin-hadoop3 spark
hdfs dfs -mkdir -p /spark-jars /spark-logs
jar cv0f ~/spark-libs.jar -C ~/spark/jars/ .
hdfs dfs -put -f ~/spark-libs.jar /spark-jars/
start-history-server.sh
```

### 6.4 Verification

The cluster was verified by checking the running daemons with `jps` on each node, confirming two live DataNodes with `hdfs dfsadmin -report`, viewing the NameNode (port 9870) and ResourceManager (port 8088) web interfaces, and running the Hadoop word-count example and the Spark Pi example on YARN.

## 7. Implementation

### 7.1 Parsing the PGN files (`parser/parse_pgn.py`)

The raw file is read as a stream (`zstdcat file | python3 parse_pgn.py`), so memory use stays small regardless of file size. For each game the parser:

1. Collects the header tags into a dictionary.
2. Reads the move text, strips comments and move numbers, and counts the half-moves (plies).
3. Detects the game speed (UltraBullet, Bullet, Blitz, Rapid, Classical, Correspondence) from the event name.
4. Writes one row per game to `games.csv`.
5. For games that contain both clock and engine evaluation comments, writes one row per move to `moves.csv`, including the mover's remaining time and the evaluation loss of the move.

**games.csv columns:** `game_id, date, time, speed, time_control, white_elo, black_elo, result, termination, eco, opening, plies, has_clock, has_eval`

**moves.csv columns:** `game_id, ply, color, clock_sec, eval_cp, loss_cp, mover_elo, speed`

A move is counted as a **blunder** when `loss_cp` is at least 200 centipawns, meaning the mover's position became about two pawns worse according to the engine evaluation. Forced-mate evaluations are capped at ±1000 centipawns. This threshold is a common convention, not the only possible definition, and the blunder percentages in this report depend on it.

### 7.2 Loading into HDFS

```bash
hdfs dfs -mkdir -p /chess/raw
hdfs dfs -D dfs.blocksize=33554432 -put -f data/games.csv data/moves.csv /chess/raw/
hdfs fsck /chess/raw -files -blocks -locations
```

A block size of 32 MB (instead of the 128 MB default) splits the files into more blocks so that both workers take part in the processing. With a replication factor of 2, every block is stored on both workers, which `fsck` confirms.

### 7.3 ETL job (`spark-jobs/etl.py`)

The ETL job reads the CSV files from HDFS and writes cleaned, typed **Parquet** files, a compressed columnar format that Spark reads much faster than CSV. It:

- casts text columns to integers and removes games with a missing rating or result;
- derives `winner` (white, black, draw) and `white_score` (1, 0.5 or 0);
- derives `moves` (full moves), `hour` of day, and `game_date`;
- derives `avg_elo`, `elo_diff` and an `elo_bracket` (below 1000, 1000–1399, 1400–1799, 1800–2199, 2200 and above);
- derives `opening_family`, the opening name before the colon (for example Sicilian Defense from Sicilian Defense: Najdorf Variation).

### 7.4 Analysis job (`spark-jobs/analysis.py`)

The analysis job reads the Parquet data, caches the games table in executor memory because it is reused by many queries, and computes ten result tables using Spark DataFrame operations (`groupBy`, `agg`, `percentile_approx` and conditional expressions). Groups smaller than a minimum game count (the `MIN_GAMES` constant in the script) are left out of the opening, rating-difference and time-pressure tables so that percentages are not based on a handful of games. Each result is written to HDFS as a single CSV and exported with `hdfs dfs -getmerge`.

| # | Result table | Computation |
|---|--------------|-------------|
| 1 | summary | Total games, average moves, average rating, white, draw and black percentages |
| 2 | opening_winrates | Result percentages per opening family and rating bracket (minimum game count applied) |
| 3 | opening_popularity | Number and share of games per opening family |
| 4 | game_length | Average and median moves per time control |
| 5 | color_by_bracket | White, draw and black percentages per rating bracket |
| 6 | terminations | Number of games per termination type and speed |
| 7 | hourly_activity | Number of games and average rating per hour of the day (UTC) |
| 8 | rating_upsets | Actual white score versus the Elo-expected score per rating-difference bucket |
| 9 | time_pressure | Blunder percentage per remaining-clock bucket and speed (0–9 s, 10–29 s, 30–59 s, 60–179 s, 180 s and above) |
| 10 | blunders_by_rating | Blunder percentage per rating bracket of the mover |

Analyses 9 and 10 use only the subset of games that include both clock and engine evaluation data.

### 7.5 Dashboard (`dashboard/app.py`)

The user interface is a Streamlit application that reads the exported CSV files and draws interactive Plotly charts. It has six pages:

| Page | Content |
|------|---------|
| Overview | Key figures, games by hour of day, how games ended |
| Openings | Result split of the most played openings for a selectable rating bracket, plus overall popularity |
| Game length | Average moves by time control |
| Colour and rating | Results by rating bracket and the actual versus expected score by rating difference |
| Time pressure | Blunder rate by time left on the clock |
| Cluster | Pipeline summary and links to the HDFS, YARN and Spark History web interfaces |

## 8. Results and Discussion

### 8.1 Summary of findings

| # | Analysis | Key finding |
|---|----------|-------------|
| 1 | Overall summary | About 4.5 million games; white wins about 50.4 percent, draws about 5.1 percent |
| 2 | Opening results by rating bracket | Win rates vary significantly by opening and skill level |
| 3 | Most popular openings | The Sicilian Defense is the most played, at about 13.5 percent of games |
| 4 | Game length by time control | Classical games average about 42 moves; Bullet about 25 |
| 5 | Colour advantage by rating bracket | White's advantage shrinks slightly at higher ratings |
| 6 | How games end | Time forfeits dominate in Bullet; normal endings dominate in Classical |
| 7 | Activity by hour (UTC) | Peak activity between 13:00 and 15:00 UTC in the sampled period. |
| 8 | Rating upsets | Actual win rates closely follow the Elo-expected curve |
| 9 | Time pressure and blunders | The blunder rate rises as the clock runs down and sharply below 10 seconds |
| 10 | Blunder rate by rating | Lower-rated players blunder about five times more often than players rated 2200 and above |

### 8.2 Discussion

The findings below describe patterns in one month of data. They show association, not proof of cause, and the explanations offered are possible interpretations rather than tested conclusions.

- **First-move advantage.** White's small edge (about 50.4 percent) is consistent with the first-move advantage generally discussed for chess. The edge shrinking slightly at higher ratings may mean that stronger players handle the second-player role better, but this analysis does not test that.
- **Time control shapes the game.** Faster time controls produce shorter games that more often end on the clock, while slower ones more often end through normal play. A likely reason is that less thinking time makes running out of time a larger risk than being checkmated.
- **Ratings predict results.** The close agreement between actual scores and the Elo-expected curve suggests the rating system is a reliable predictor on this dataset.
- **Pressure and errors.** Blunders become more frequent when little time is left, and lower-rated players blunder more often overall. Because the blunder analysis relies on engine evaluations that exist only for a subset of games, these rates may not represent all games (see Section 13).

## 9. Validation and Performance Evaluation

### 9.1 Validation

| Check | Method | Result |
|-------|--------|--------|
| Row counts | Parser game count compared with ETL output | Match, apart from dropped rows with missing ratings |
| Colour balance | Overview page | White about 50.4 percent (expected range 50–52 percent) |
| Rating upsets | Actual versus expected chart | Actual tracks the Elo-expected curve |
| Time pressure | Blunder chart | Blunder rate rises as the clock runs low, sharply below 10 seconds |
| Cluster usage | Spark UI Executors tab | Executors run on both workers |

### 9.2 Scaling experiment

The analysis job was run with one executor and then with two, and the durations were read from the Spark History Server.

| Executors | Duration | Speed-up |
|-----------|----------|----------|
| 1 | **[FILL: X.X min]** | 1.0x |
| 2 | **[FILL: Y.Y min]** | **[FILL: Z.Zx]** |

The speed-up is the 1-executor duration divided by the 2-executor duration. On a small dataset the gain is modest because job start-up time dominates, and all three nodes are virtual machines sharing the resources of their host PCs, so these timings indicate the trend rather than the performance of dedicated servers. The benefit of more executors is expected to grow with data size.

## 10. Basic Commands Used

| Command | Purpose |
|---------|---------|
| `jps` | List running Java daemons (NameNode, DataNode, ResourceManager, NodeManager) |
| `start-dfs.sh`, `start-yarn.sh` | Start the HDFS and YARN daemons on all nodes |
| `hdfs namenode -format` | Initialise the NameNode metadata (run once) |
| `hdfs dfs -mkdir -p`, `-put`, `-get`, `-getmerge` | Create directories, upload files, download files, merge result parts into one file |
| `hdfs dfs -ls -h`, `-du -h`, `-cat`, `-rm -r` | List, show sizes, print and delete files |
| `hdfs fsck /path -files -blocks -locations` | Check file health and show where each block replica is stored |
| `hdfs dfsadmin -report` | Show cluster capacity and live or dead DataNodes |
| `yarn node -list`, `yarn application -list` | Show NodeManagers and applications |
| `spark-submit job.py` | Submit a Spark application to YARN (`--num-executors`, `--executor-memory` control resources) |
| `pyspark` | Interactive Spark shell |
| `start-history-server.sh` | Start the Spark History Server |

## 11. Design Decisions

| Decision | Reason |
|----------|--------|
| Replication factor 2 | The cluster has two DataNodes; the default of 3 would leave blocks under-replicated |
| 32 MB block size for the data files | Produces more blocks so both workers share the work, making parallelism visible |
| Spark on YARN rather than standalone mode | Reuses the Hadoop resource manager and needs Spark installed only on the master |
| Parquet for intermediate data | Columnar, compressed and schema-aware, so jobs read less data and run faster |
| `cache()` on the games table | The table is reused by many analyses and is read from HDFS only once |
| 16 shuffle partitions | The default of 200 creates too many tiny tasks on a small cluster |
| Spark jar archive in HDFS | Avoids uploading Spark's jars on every job submission |
| Hadoop and Spark only, no HBase | Keeps the setup reliable; HBase was deferred as an optional extension |

## 12. Challenges and Solutions

Setting up a multi-node cluster has well-known pitfalls. The table lists the main ones for this project and how the setup addresses them.

| Challenge | Why it matters | Solution |
|-----------|----------------|----------|
| Hostname mapped to the loopback address (127.0.1.1) in `/etc/hosts` | Hadoop daemons bind to loopback and other nodes cannot connect | Removed that line and mapped master, worker1 and worker2 to their real LAN addresses on every node |
| Environment variables not visible to daemons started over SSH | `start-dfs.sh` launches daemons through non-interactive SSH sessions | Set `JAVA_HOME` in `hadoop-env.sh` and placed the variables at the top of `~/.bashrc` |
| Reformatting the NameNode after data exists | DataNodes refuse to start because of a cluster ID mismatch | Formatted the NameNode only once |
| YARN killing containers on small VMs | Memory limits are easily exceeded with limited RAM | Sized YARN memory to each VM, disabled the virtual-memory check and used 1 GB executors |
| Too many tiny Spark tasks | The default of 200 shuffle partitions is wasteful on a small cluster | Set `spark.sql.shuffle.partitions` to 16 |
| Nodes unable to reach each other | Network isolation or changing IP addresses break the cluster | Used bridged adapters on one local network and fixed IP addresses |
| Large input and slow parsing | Loading a whole file into memory would not scale | The parser streams the file in a single pass and was first tested on a small slice |

## 13. Limitations and Future Work

- **Parser is single-node.** `parse_pgn.py` runs only on the master. A future improvement is to parse the raw PGN inside Spark using a custom record delimiter so that this step is distributed too.
- **Subset of one month.** Only about 4.5 million games from the October 2026 file were analysed, taken as a contiguous portion and not a random sample. The findings describe that portion, and patterns such as activity by hour or day may differ over the full month. Processing the full file would need more storage and a distributed parser.
- **Subset for time-pressure analysis.** Analyses 9 and 10 cover only games that include clock and engine evaluation data, which are a subset of all games and may differ from the rest (for example in player strength or time control).
- **Descriptive analysis only.** The findings show patterns and associations; they do not establish causes, and no statistical significance tests were performed.
- **Blunder definition.** The 200-centipawn threshold is one convention; other thresholds would change the blunder percentages.
- **Virtualised cluster.** The nodes are virtual machines sharing their host PCs, so timings reflect that environment.
- **No HBase.** HBase was considered for fast key-value lookups of opening statistics but was deferred to keep the cluster setup reliable.
- **Batch results only.** The dashboard shows precomputed results; a streaming pipeline (for example with Kafka) could provide live statistics.

## 14. Conclusion

The project delivered a working big data system on a three-node Hadoop and Spark cluster. Raw Lichess game files were parsed, distributed and replicated in HDFS, processed in parallel with Spark on YARN, and presented through a dashboard. The ten analyses describe white's small first-move advantage, the effect of time control on game length, the link between time pressure and blunders, and the close match between ratings and results. The team also gained hands-on experience installing and configuring Hadoop and Spark, working with their command-line tools, and handling the practical problems of a multi-node setup.

## 15. Team Contributions

Each member led the area below.

| Member | Role | Contribution |
|--------|------|--------------|
| Sanchit Agrawal | A – Cluster setup | VirtualBox VMs and networking, SSH, Hadoop and Spark installation and configuration, History Server, cluster configuration files |
| Omansh Thakur | B – Data parsing and HDFS | Data download, PGN parser, loading data into HDFS, `fsck` evidence |
| Saumya Prakash Narayan | C – Spark jobs | ETL and analysis jobs, validation, scaling experiment |
| Pankaj Jaiswal | D – Dashboard and documentation | Streamlit dashboard, README report, screenshots, demo preparation |

## 16. Reproducing the Project

### 16.1 Repository structure

```
chess-analytics/
├── cluster-setup/          # Hadoop and Spark config files
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   ├── yarn-site.xml
│   ├── hadoop-env.sh
│   ├── spark-defaults.conf
│   ├── spark-env.sh
│   └── workers
├── parser/
│   └── parse_pgn.py        # PGN to CSV streaming parser
├── spark-jobs/
│   ├── etl.py              # CSV to cleaned Parquet
│   └── analysis.py         # 10 analytical queries
├── dashboard/
│   ├── app.py              # Streamlit web UI
│   └── requirements.txt
├── results/                # Exported result CSVs (committed)
├── sample-data/            # Small sample for testing
├── .gitignore
└── README.md               # This report
```

### 16.2 Run order

Set up the cluster as in Section 6, then:

**Parse the data**

```bash
cd ~/chess-analytics
zstdcat data/lichess_db_standard_rated_2026-10.pgn.zst | python3 parser/parse_pgn.py data/games.csv data/moves.csv
```

**Load into HDFS**

```bash
hdfs dfs -mkdir -p /chess/raw
hdfs dfs -D dfs.blocksize=33554432 -put -f data/games.csv data/moves.csv /chess/raw/
```

**Run the Spark jobs**

```bash
spark-submit ~/chess-analytics/spark-jobs/etl.py
spark-submit ~/chess-analytics/spark-jobs/analysis.py
```

**Export the results**

```bash
mkdir -p ~/chess-analytics/results
for d in summary opening_winrates opening_popularity game_length color_by_bracket terminations hourly_activity rating_upsets time_pressure blunders_by_rating; do
    hdfs dfs -getmerge /chess/results/$d ~/chess-analytics/results/$d.csv
done
```

**Launch the dashboard**

```bash
pip3 install --user streamlit pandas plotly
streamlit run ~/chess-analytics/dashboard/app.py --server.address 0.0.0.0 --server.port 8501 --server.headless true
```

Open `http://master:8501`, or `http://<master-IP>:8501` from any PC on the network.

The dataset files are not stored in the repository because of their size (GitHub rejects files above 100 MB). A small sample is provided in `sample-data/`, and the exported result tables are in `results/`.

## 17. References

1. Lichess open database, https://database.lichess.org (CC0 licence).
2. Apache Hadoop 3.3.6 documentation, https://hadoop.apache.org.
3. Apache Spark 3.5.3 documentation, https://spark.apache.org.
4. Streamlit documentation, https://streamlit.io.
5. Portable Game Notation specification (PGN standard).
