# **📘 Module 8 – Data Quality & Monitoring (Week 12\)**

This page is the handbook for Week 12 – Data Quality & Monitoring in the Data Engineering & Analytics module.

🎯 **Learning Objectives**  
 By the end of this week, you will be able to:

* Explain the data quality lifecycle and where checks belong in a modern Medallion architecture.

* Implement data validation with:

  * Great Expectations (GX) on Spark data (Microsoft Fabric \+ Amazon Sales dataset).

  * Pydantic for config and row-level validation in Python.

* Describe data observability and when to use tools like Monte Carlo and Datadog.

* Add CI/CD around data pipelines using GitHub Actions or Jenkins, including automated GE checkpoints.

  ---

  # **1\. Big Picture: Data Quality & Observability**

  ## **1.1 Data Quality vs. Data Observability**

**Data Quality (DQ):** Are the values correct, complete, consistent, and fit for purpose?  
 Examples: no null IDs, valid email format, totals match source system.

📺 [Data Quality Explained](https://youtu.be/5HcDJ8e9NwY?si=BBlFx2cI8-yczVjb)

**Data Observability:** Can we see when something breaks in the data or pipelines in real time?  
 Focus on freshness, volume, schema, distribution, lineage and alerts.

A healthy platform needs both:

* DQ rules (Great Expectations, Collibra DDQ, Pydantic models)

* Observability & monitoring (Monte Carlo, Datadog, Slack alerts, dashboards)

  ---

  # **2\. Data Validation**

  # **2.1 Great Expectations (GX)**

Great Expectations is an open-source framework for declarative data tests. You describe how the data should look, and GX validates your datasets against those expectations.

## **2.1.1 Core Concepts**

## **2.1.2 Typical GX Workflow**

* Connect to data (Spark, SQL, pandas, etc.).

* Create an Expectation Suite:

  * Start with profiling (auto-suggested expectations).

  * Then refine manually.

* Define a Checkpoint – which suite(s) \+ what batch.

* Run validations:

  * Locally, in a notebook.

  * Inside pipeline steps (Fabric, ADF, Airflow, etc.).

* Publish Data Docs and send alerts if validation fails.

  ## **2.1.3 Great Expectations – Watchlist & Readlist**

  📺[Great Expectations (GX) for DATA Testing \- Introduction](https://www.youtube.com/playlist?list=PLYDwWPRvXB8_XOcrGlYLtmFEZywOMnGSS)  
  📺[Implementing Data Quality in Python w/ Great Expectations](https://www.youtube.com/playlist?list=PLYDwWPRvXB8_XOcrGlYLtmFEZywOMnGSS)

  📚GE Official docs – [https://docs.greatexpectations.io/](https://docs.greatexpectations.io/)  
  📚Datacamp tutorial – [https://www.datacamp.com/tutorial/great-expectations-tutorial](https://www.datacamp.com/tutorial/great-expectations-tutorial)  
  📚Microsoft Fabric example – [https://devblogs.microsoft.com/ise/data-validations-with-great-expectations-in-ms-fabric](https://devblogs.microsoft.com/ise/data-validations-with-great-expectations-in-ms-fabric)

  ---

  # **⭐ HOMEWORK 1 — Great Expectations with Pandas (Local Project)**

  ### **🎯 Goal**

Understand the fundamentals of Great Expectations by validating a CSV file locally using Pandas.

### **✔ Tasks**

1. Load your CSV (Amazon subset or any small dataset) using Pandas.

2. Initialize a Great Expectations project.

3. Create an expectation suite.

4. Add at least **5 rules**, such as:

   * `order_id` cannot be null

   * `quantity > 0`

   * `price >= 0`

   * `ship_date >= order_date`

   * `country` in the allowed set

5. Run validation.

6. Export results to:

   * `expectations.json`

   * `gx_report.json`

   ### **✔ Output**

1. `/homework1/`  
2.   `expectations.json`  
3.   `gx_report.json`  
4.   `script.py or notebook`  
     
   ---

   # **2.2 Pydantic for Schema & Config Validation**

Pydantic is a Python library that uses type hints to validate data. It is ideal for:

* Validating pipeline configuration (paths, thresholds, email lists, etc.).

* Enforcing schemas for JSON payloads, API responses, or intermediate objects.

* Building reliable internal DTOs (Data Transfer Objects) for your data apps.

We focus on **Pydantic v2**.

## **2.2.1 Pydantic – Watchlist & Readlist**

📺[Pydantic V2 - Full Course - Learn the BEST Library for Data Validation and Parsing](https://youtu.be/7aBRk_JP-qY)  
📺[Pydantic Tutorial • Solving Python's Biggest Problem](https://youtu.be/XIdQ6gO3Anc)

📚Pydantic Models – [https://docs.pydantic.dev/latest/concepts/models/](https://docs.pydantic.dev/latest/concepts/models/)  
📚Validators – [https://docs.pydantic.dev/latest/concepts/validators/](https://docs.pydantic.dev/latest/concepts/validators/)

📚Real Python intro – [https://realpython.com/python-pydantic/](https://realpython.com/python-pydantic/)

---

# **⭐ HOMEWORK 2 — Pydantic (Row-Level \+ Config Validation)**

### **🎯 Goal**

Use Pydantic models to validate rows and pipeline configuration.

### **✔ Tasks**

1. Define an `Order` model with constraints:

   * `quantity > 0`

   * `price >= 0`  
       
   * etc.

2. Load CSV and validate each row.

3. Write valid and invalid rows to:

   * `valid_rows.csv`

   * `invalid_rows.csv`

4. Create a `config.yaml` with thresholds & allowed values.

5. Validate config using Pydantic.

   ### **✔ Output**

5. `/homework2/`  
6.   `valid_rows.csv`  
7.   `invalid_rows.csv`  
8.   `config.yaml`  
9.   `validation.py`  
     
   ---

   # **3\. Data Observability**

Data observability tools like Monte Carlo and Datadog help answer:

“Is my data pipeline healthy right now and who is impacted if something goes wrong?”

Key monitored pillars:

* Freshness

* Volume

* Schema

* Distribution

* Lineage

📺 [What is Data Observability?](https://youtu.be/jfg9wBJBtKk?si=ezqKJYbpo7my2_rk)

---

# **3.1 Monte Carlo**

Monte Carlo is a Data \+ AI Observability platform that connects to your data warehouses, lakes, ETL tools, and BI dashboards.

## **3.1.1 What Monte Carlo Monitors**

* Freshness, volume, nulls, uniqueness

* Incidents

* Lineage

* SLAs/SLOs

  ## **3.1.2 Monte Carlo – Watchlist & Readlist**

📺 [2025: Monte Carlo Data \+ AI Observability Platform Demo](https://www.youtube.com/watch?v=MmvZY1gTAy4)

📚 [https://www.montecarlodata.com/data-observability-overview/](https://www.montecarlodata.com/data-observability-overview/)  
 📚 [https://docs.getmontecarlo.com/docs/architecture](https://docs.getmontecarlo.com/docs/architecture)  
 📚 [https://www.montecarlodata.com/blog-what-is-data-observability/](https://www.montecarlodata.com/blog-what-is-data-observability/)

# **⭐ Exercise — End-to-end observability-Demo**

⚒️ https://www.montecarlodata.com/platform/data-quality/

---

# **3.2 Datadog for Data Stack Monitoring**

Use Datadog to:

* Collect logs

* Monitor pipeline metrics

* Create alerts

* Track Spark jobs

* Build dashboards

  ## **3.2.1 Datadog — Watchlist & Readlist**

📺 [Datadog 101 Course | Datadog Tutorial for Beginners | SRE | DevOps](https://www.youtube.com/watch?v=Js06FTU3nXo)  
 📺 [L01 - Datadog Log Intro: Logging Philosophy](https://youtu.be/-64x697bELI?si=2HI2oR4sZ7a_bFDi)

📘 Courses:

* 📒 [https://learn.datadoghq.com/courses/log-explorer](https://learn.datadoghq.com/courses/log-explorer)

* 📒 [https://learn.datadoghq.com/courses/getting-started-metrics](https://learn.datadoghq.com/courses/getting-started-metrics)

* 📒 [https://learn.datadoghq.com/courses/getting-started-monitors](https://learn.datadoghq.com/courses/getting-started-monitors)

📚 [https://docs.datadoghq.com/logs/log\_configuration/pipelines/](https://docs.datadoghq.com/logs/log_configuration/pipelines/)  
 📚 [https://docs.datadoghq.com/logs/log\_configuration/logs\_to\_metrics/](https://docs.datadoghq.com/logs/log_configuration/logs_to_metrics/)  
 📚 [https://www.datadoghq.com/architecture/a-guide-to-log-management-indexing-strategies-with-datadog/](https://www.datadoghq.com/architecture/a-guide-to-log-management-indexing-strategies-with-datadog/)

---

# **4\. CI/CD for Data Pipelines**

CI/CD for data teams means:

* Automatically testing pipeline code

* Running GE checks

* Deploying notebooks, SQL objects, dbt models

We focus on **GitHub Actions** and **Jenkins**.

---

# **4.1 GitHub Actions**

GitHub Actions uses YAML workflows stored in `.github/workflows/`.

## **4.1.1 Watchlist & Readlist**

📺 [GitHub Actions Tutorial - Basic Concepts and CI/CD Pipeline with Docker](https://youtu.be/R8_veQiYBjI?si=CDcxTTH4CM1qloK3)  
📺 [GitHub Actions CI/CD pipeline | Step by Step guide](https://youtu.be/a5qkPEod9ng?si=t3s5FpLeiLqRLiOF)

📚 GitHub blog:  
 [https://github.blog/enterprise-software/ci-cd/keeping-your-data-pipelines-healthy-with-the-great-expectations-github-action/](https://github.blog/enterprise-software/ci-cd/keeping-your-data-pipelines-healthy-with-the-great-expectations-github-action/)

📚 GE Blog:  
 [https://greatexpectations.io/blog/github-actions/](https://greatexpectations.io/blog/github-actions/)

📚 Datacamp:  
 [https://www.datacamp.com/blog/ci-cd-in-data-engineering](https://www.datacamp.com/blog/ci-cd-in-data-engineering)

---

# **⭐ HOMEWORK 3 — GitHub Actions CI Pipeline for Data Quality**

### **🎯 Goal**

Run Great Expectations automatically on every GitHub push.

### **✔ Tasks**

1. Create a GitHub repo.

2. Add your validation script under `/src/`.

3. Create `.github/workflows/dq-check.yml` that:

   * installs Python

   * installs dependencies

   * runs your validation

   * fails pipeline if DQ fails

4. Confirm: pipeline turns **red** on failure.

   ### **✔ Output**

* Repo link

* Screenshot of failed/passed workflow

  ---

  # **⭐ HOMEWORK 4 — Slack Notification for Data Quality Failures**

  ### **🎯 Goal**

Send Slack alerts when DQ validation fails.

### **✔ Tasks**

1. Create an **Incoming Webhook** in Slack.

2. Write a Python function that sends a message to Slack on failure.

3. Integrate this function into your validation script.

4. Add a Slack notification step to GitHub Actions.

   ### **✔ Output**

* Slack message screenshot

* Updated workflow file

  ---

  # **4.2 Jenkins**

📺 [Master Jenkins Pipelines | Step by Step Tutorial for Beginners | KodeKloud](https://youtu.be/hgUGblYj-JQ?si=48l6S-xuwNtUZQ8X)  
 📺 [Complete Jenkins Pipeline Tutorial | Jenkinsfile explained | KodeKloud](https://youtu.be/EzgCoOQvOf0?si=KTuPIWKuiF6AHNWU)

📚 [https://www.jenkins.io/doc/book/pipeline/](https://www.jenkins.io/doc/book/pipeline/)  
 📚 [https://www.jenkins.io/doc/pipeline/examples/](https://www.jenkins.io/doc/pipeline/examples/)

---

# **5\. Collibra DDQ – Profiling & Quality Controls** 

**📺**[Collibra | Streamline Your Customer Product Sales Data with Collibra | Part - 1](https://www.youtube.com/watch?v=BtKb1uKLH18&list=PLfMV70VIUv4smY3BlLlUs98_PUnxYYD_x)

**📺**[Collibra Data Quality & Observability: Out-of-the-box Features](https://youtu.be/T-MBwqokhkQ?si=KVPRrpKhTp1pvH7q)

**📚**https://www.collibra.com/blog/what-is-data-quality

---

# **🚀 Final Project (In-Class)**

A short end-to-end Microsoft Fabric project where you will apply Week 12 concepts by validating the **Amazon Sales** dataset using **Great Expectations on Spark** and sending **Slack alerts** when validation fails.

**Good Luck\!**

---

