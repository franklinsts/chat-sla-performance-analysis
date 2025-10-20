# 🤖 Chatbot SLA & Productivity Analytics  

**Author:** Franklin Santana dos Santos  
**Language:** SQL (Databricks / Spark SQL)  
**Topic:** Operational performance analysis and chat support  

---

## 🏁 Objective  

Develop a comprehensive analysis of **chatbot and human agent performance**, evaluating:  
- volume of tickets offered and handled;  
- productivity per agent and queue;  
- SLA compliance;  
- average response and handling times;  
- accumulated backlog per time window.  

The project aims to measure **operational efficiency** and identify **bottlenecks in digital support workflows**.

---

## 🧩 Data Sources  

| Source | Description |
|-------|------------|
| `analytics.chatbot_interactions` | Interactions between customers and chatbot, including action type and productivity. |
| `analytics.chat_sessions` | Chat session records, including start time, end time, and agent response timestamps. |
| `support.ticket_group_history` | Ticket history by queue and entry timestamp. |
| `support.ticket_details` | Additional ticket information (channel, product, segment, etc.). |

---

## ⚙️ Main SQL Techniques Used  

- **CTEs (Common Table Expressions)** to structure the analysis pipeline;  
- **Window functions** (`row_number()`, `lead()`) to identify relevant actions and sessions;  
- **Dynamic case statements** to handle day, week, and month granularities;  
- **Timestamp conversion** with `unix_timestamp()` and calculation of minutes;  
- **Conditional filtering with `FILTER (WHERE ...)`** to calculate SLA compliance and backlog;  
- **Data cleaning and deduplication** for sessions and tickets.  

---

## 🧮 Calculated Metrics  

| Metric | Description |
|--------|------------|
| **handled** | Number of tickets processed. |
| **productive_actions** | Total number of productive actions. |
| **productivity_per_agent** | Average productive actions per agent. |
| **avg_response_time** | Average response time (minutes). |
| **avg_handling_time** | Average handling time by agent (minutes). |
| **sla_compliance** | Number of tickets handled within SLA. |
| **backlog_24h / 48h / 72h / +72h** | Number of pending tickets per time window. |
| **avg_total_handling_time** | Average total handling time per offered ticket. |

---

## 📈 Example Results  

| Day | Tickets Offered | SLA Compliance (%) | Productivity per Agent | Average Handling Time (min) | Backlog +72h |
|-----|----------------|------------------|-----------------------|----------------------------|--------------|
| 2025-03-01 | 1,240 | 87% | 42.3 | 18.5 | 34 |
| 2025-03-02 | 1,130 | 84% | 38.9 | 19.8 | 49 |
| 2025-03-03 | 1,550 | 91% | 44.1 | 17.2 | 27 |

*(illustrative data)*

---

## 💼 Business Insights  

- Average handling time (AHT) remains below **20 minutes**, indicating good efficiency.  
- **SLA compliance** ranges from **84–91%**, with fluctuations during peak demand.  
- The **“Complaints”** queue has the highest backlog above 72h, suggesting headcount redistribution.  
- **Handled per agent** metric correlates directly with overall productivity.  

---

## 🧰 Technologies Used  

- **SQL (Spark / Databricks)**  
- **Azure Databricks Workspace**  
- **Google Sheets + Looker Studio** for results visualization  

---

## 🧱 Repository Structure  

📁 chatbot-sla-performance-analysis/
├── query.sql # Main SQL analysis query

├── README.md # Full project documentation

├── sample_results.csv # Example dataset (optional)

└── dashboard_preview.png # Screenshot of dashboard (optional)


---

## 🚀 Next Steps  

1. Automate export of results to **Google Sheets** using Python;  
2. Build a **Looker Studio dashboard** for visual tracking;  
3. Add historical comparison (Month vs Month) and seasonality analysis;  
4. Publish a LinkedIn article explaining methodology and operational impact.  

---

## 🧠 Technical Notes  

The query is designed for **Databricks (Spark SQL)** and uses dynamic parameters:  
- `:granularity` → defines grouping level (`Day`, `Week`, `Month`);  
- `:date.min` / `:date.max` → define date ranges;  

These parameters make the analysis flexible and applicable to multiple periods.

---

## 📎 Contact  

👤 **Franklin Santana dos Santos**  
🔗 [GitHub - franklinsts](https://github.com/franklinsts)  
💼 Senior Customer Experience Analyst • Focused on Data Analysis  
📊 SQL | Databricks | Looker Studio | Google Sheets | Data Automation  

