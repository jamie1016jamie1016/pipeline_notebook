
# Week 5 Data Pipeline Maintenance

## Responsibilities Overview

### **Business Area Pipelines**
1. **Profit**
   - Unit-level profit needed for experiments.
   - Aggregate profit reported to investors.
2. **Growth**
   - Aggregate growth reported to investors.
   - Daily growth needed for experiments.
3. **Engagement**
   - Aggregate engagement reported to investors.

---

## **Pipeline Ownership**

### **Profit Pipelines**
- **Unit-level profit needed for experiments**:
  - **Primary Owner**: Engineer A
  - **Secondary Owner**: Engineer B
- **Aggregate profit reported to investors**:
  - **Primary Owner**: Engineer B
  - **Secondary Owner**: Engineer A

### **Growth Pipelines**
- **Aggregate growth reported to investors**:
  - **Primary Owner**: Engineer C
  - **Secondary Owner**: Engineer D
- **Daily growth needed for experiments**:
  - **Primary Owner**: Engineer D
  - **Secondary Owner**: Engineer C

### **Engagement Pipelines**
- **Aggregate engagement reported to investors**:
  - **Primary Owner**: Engineer A
  - **Secondary Owner**: Engineer C

---

## **On-Call Schedule**

- **Principles for Fair Scheduling**:
  - Rotations occur weekly to ensure equal distribution of workload.
  - Secondary owners are backup on-call in case the primary owner is unavailable.
  - Holiday coverage rotates to avoid overburdening any one team member.

### **Example Schedule**

| Week         | Primary On-Call | Secondary On-Call |
|--------------|------------------|-------------------|
| Week 1       | Engineer A       | Engineer B        |
| Week 2       | Engineer B       | Engineer C        |
| Week 3       | Engineer C       | Engineer D        |
| Week 4       | Engineer D       | Engineer A        |
| Holidays     | Rotates based on volunteer scheduling. |

---

## **Run Books for Investor-Facing Pipelines**

### **1. Aggregate Profit Pipeline**
- **Potential Issues**:
  - **Data freshness**: Delayed upstream data sources can impact reporting.
  - **Data accuracy**: Errors in currency conversions or rounding inconsistencies.
  - **Pipeline failures**: Timeout errors or misconfigured ETL jobs.

### **2. Aggregate Growth Pipeline**
- **Potential Issues**:
  - **Data schema changes**: Upstream schema modifications causing pipeline errors.
  - **Duplicate records**: Skewed growth numbers due to unhandled duplicates.
  - **Missing data**: Gaps in data ingestion leading to incomplete metrics.

### **3. Aggregate Engagement Pipeline**
- **Potential Issues**:
  - **Metric definitions**: Misalignment of engagement definitions with investor expectations.
  - **Volume spikes**: High traffic days causing processing delays or failures.
  - **Broken transformations**: ETL logic bugs impacting engagement metric accuracy.

---

## Conclusion
This plan ensures clear ownership, fair scheduling, and thorough preparation for maintaining critical business pipelines. Proper run books and fair on-call schedules will minimize downtime and ensure investor confidence in the metrics.
