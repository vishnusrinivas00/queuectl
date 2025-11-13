# **QueueCTL — Background Job Queue System**

QueueCTL is a lightweight CLI-based background job processor built for the **Flamapp Backend Developer Internship Assignment**.
It supports background job execution, multiple worker processes, automatic retries with exponential backoff, per-job timeouts, a Dead Letter Queue, and persistent storage using SQLite.

I designed the system to be simple, modular, and easy to test while still covering the core concepts expected in a production-style job queue.

---

# **1️) Setup Instructions**

### **Clone the repository**

```bash
git clone https://github.com/<your-username>/queuectl.git
cd queuectl
```

### **Create & activate virtual environment**

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
```

### **Install QueueCTL**

```bash
pip install -e .
```

### **Verify installation**

```bash
queuectl --help
```

---

# **2️) Usage Examples**

### **Enqueue a job**

```bash
queuectl enqueue "{\"id\":\"job1\",\"command\":\"echo Hello Vishnu\"}"
```

### **Start workers**

```bash
queuectl worker start --count 2
```

### **Check status**

```bash
queuectl status
```

Example output:

```json
{
  "pending": 0,
  "processing": 0,
  "completed": 1,
  "failed": 0,
  "dead": 0,
  "workers": 2
}
```

### **List jobs**

```bash
queuectl list --state completed
```

### **View DLQ**

```bash
queuectl dlq list
```

### **Retry DLQ job**

```bash
queuectl dlq retry job1
```

### **Metrics**

```bash
queuectl metrics
```

### **Timeout example**

```bash
queuectl enqueue "{\"id\":\"slowjob\",\"command\":\"powershell -Command Start-Sleep -Seconds 10\",\"timeout_seconds\":5}"
```

---

# **3️) Architecture Overview**

## **Job Lifecycle**

A job moves through these states:

| State        | Description                         |
| ------------ | ----------------------------------- |
| `pending`    | Waiting to be picked up by a worker |
| `processing` | Worker is running it                |
| `completed`  | Job completed successfully          |
| `failed`     | Failed but still retryable          |
| `dead`       | Permanent failure (moved to DLQ)    |

---

## **Workers**

Workers:

* Poll for `pending` jobs
* Safely claim jobs using a DB-level row lock
* Execute commands using `subprocess.run()`
* Handle retries and exponential backoff
* Move permanently failed jobs into DLQ
* Update timing + metrics fields
* Gracefully stop via OS signals (`Ctrl+C`)

Multiple workers can run in parallel without conflict.

---

## **Data Persistence**

All job state is stored in **SQLite**:

* `jobs` — active jobs
* `dlq` — permanently failed jobs
* `config` — runtime settings like backoff base
* `workers` — active worker PIDs

SQLite ensures persistence even when everything shuts down.

---

## **Why SQLite?**

* Zero setup
* ACID transactions
* Ideal for CLI tools
* Prevents duplicate job execution with row locks

---

# **4️) Assumptions & Trade-offs**

### **Assumptions**

* Jobs are shell commands executed via `subprocess`.
* Workers run on a single machine.
* Per-job timeout is optional but supported.

### **Trade-offs**

* **SQLite instead of PostgreSQL** → chosen for simplicity and portability.
* **shell=True** → acceptable for assignment but not ideal for production.
* **CLI-only** → no dashboard to keep the project focused and simple.
* **Output logging limited** → stores only error and duration.

These choices keep the system achievable and maintainable within assignment constraints.

---

# **5️) Testing Instructions**

### **1. Successful job**

```bash
queuectl enqueue "{\"id\":\"ok1\",\"command\":\"echo hi\"}"
queuectl worker start --count 1
queuectl status
```

### **2. Fail + retry + DLQ**

```bash
queuectl enqueue "{\"id\":\"bad1\",\"command\":\"cmd /c exit 1\"}"
queuectl worker start --count 1
queuectl dlq list
```

### **3. Timeout test**

```bash
queuectl enqueue "{\"id\":\"slow\",\"command\":\"powershell -Command Start-Sleep -Seconds 10\",\"timeout_seconds\":5}"
queuectl dlq list
```

### **4. Metrics**

```bash
queuectl metrics
```

### **5. Persistence test**

Restart terminal and run:

```bash
queuectl status
queuectl list
queuectl dlq list
```

---

# ** Demo Video**

(https://drive.google.com/file/d/1Jx6p_94qOCyTi6Sg4us3ZenlMbe3HngF/view?usp=sharing)
to watch the demonstration

---

# ** Author**

**Vishnu Srinivas**
GitHub: [https://github.com/vishnusrinivas00](https://github.com/vishnusrinivas00)

---
