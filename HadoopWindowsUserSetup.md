<!-- UPDATE: Rewritten for Spark 4.1.1 — JDK 21, Hadoop 3.4.x winutils, added WSL 2 recommendation,
     removed unnecessary full-Hadoop setup steps, fixed contradictory JDK version references -->

# Setting Up Spark on Windows

## Recommended: Use WSL 2

The simplest way to run Spark on Windows is via **Windows Subsystem for Linux (WSL 2)** with Docker Desktop.
With this approach you do not need Hadoop or winutils at all:

1. Install [WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install) (run `wsl --install` in PowerShell as admin)
2. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/) with the WSL 2 backend enabled
3. Open your WSL terminal, clone this repo, and run `docker compose up --scale spark-worker=3`
4. Open the project in IntelliJ (it can use the WSL JDK or a Windows JDK 17+)

If you prefer a native Windows setup without WSL, follow the steps below.

---

## Native Windows Setup

*Apache Spark doesn't have its own distributed file system, so it relies on Hadoop libraries for file I/O
even in local mode on Windows. You need `winutils.exe` and `hadoop.dll` for Spark to work correctly.*

### Prerequisites

1. **Java Development Kit (JDK) 17 or 21** (Spark 4.x does NOT support Java 8 or 11)
2. The `winutils.exe` binary for Hadoop 3.4.x

### Step 1: Install JDK 21

Download and install [Eclipse Temurin JDK 21](https://adoptium.net/temurin/releases/?version=21) (recommended)
or [Oracle JDK 21](https://www.oracle.com/java/technologies/downloads/).

During installation, check the option to set `JAVA_HOME` automatically, or do it manually in Step 3.

### Step 2: Download Hadoop winutils

Download `winutils.exe` and `hadoop.dll` for Hadoop 3.4.x from the
[kontext-tech/winutils](https://github.com/kontext-tech/winutils) repository.

1. Go to `https://github.com/kontext-tech/winutils`
2. Navigate to the `hadoop-3.4.0/bin/` folder
3. Download `winutils.exe` and `hadoop.dll`
4. Create the folder `C:\hadoop\bin`
5. Place both files into `C:\hadoop\bin`

### Step 3: Set Environment Variables

1. Open the Start menu and search for **"Environment Variables"**
2. Click **"Edit the system environment variables"**
3. Click the **"Environment Variables"** button
4. Under **"System Variables"**:
   - Click **"New"** and add:
     - Variable name: `JAVA_HOME`
     - Variable value: the path to your JDK installation (e.g., `C:\Program Files\Eclipse Adoptium\jdk-21.0.6.7-hotspot`)
   - Click **"New"** again and add:
     - Variable name: `HADOOP_HOME`
     - Variable value: `C:\hadoop`
5. Find the **"Path"** variable in the System Variables list and click **"Edit"**
6. Add these two entries:
   - `%JAVA_HOME%\bin`
   - `%HADOOP_HOME%\bin`
7. Click **"OK"** to close all windows

### Step 4: Verify

Open a **new** Command Prompt or PowerShell window and run:

```
java -version
winutils.exe chmod 777 /tmp/hive
```

If both commands succeed without errors, you're ready to run Spark.

### That's It

The steps above are sufficient for Spark to run in local mode for this course.
You do **not** need to install or configure a full Hadoop distribution (HDFS, YARN, etc.).

---

**Troubleshooting:**

- `java.io.IOException: Could not locate executable winutils.exe` — `HADOOP_HOME` is not set correctly or `winutils.exe` is not in `%HADOOP_HOME%\bin`
- `UnsatisfiedLinkError: hadoop.dll` — make sure `hadoop.dll` is also in `%HADOOP_HOME%\bin` and `%HADOOP_HOME%\bin` is on your PATH
- `UnsupportedClassVersionError` — you are running an older JDK (8 or 11). Spark 4.x requires JDK 17 or 21.
