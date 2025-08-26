# 🏥 Care_Stat – A Comprehensive Healthcare Data Management System

![Care_Stat Banner](preencoded.png)

**Care_Stat** is a full-cycle relational database project built from the ground up to manage medical data in a hospital environment — from designing the relational model and collecting data, to advanced analysis and actionable insights.

---

## 🧑‍🤝‍🧑 Team Members
- **Ali Hany Ali Nosseir**
- **Ali Aboud Abdelmaksoud**

**Supervised by:**  
- Engineer Mohamed Abu Obeida  
- Engineer Mariam  

---

## 🎯 Project Objective
The healthcare sector often struggles with fragmented and inconsistent data, leading to inefficiencies in patient tracking, appointment scheduling, and financial analysis.  
**Care_Stat** addresses this by providing a **centralized, integrated database system** that enhances:
- Operational efficiency
- Quality of healthcare
- Data-driven decision-making
- Financial sustainability

---

## 🔧 What I Built (End-to-End)

✅ **Designed a complete relational database from scratch**  
- Ensured data integrity and minimized redundancy
- Implemented proper relationships (One-to-Many, Many-to-Many)
- Normalized schema for scalability and performance

📥 **Collected & cleaned realistic healthcare data**  
- Simulated a real hospital environment:
  - 10,000+ patients
  - 12 medical departments
  - Appointments, visits, medical records, and payments

💾 **Used SQL for database operations**  
- Created tables, constraints, and indexes
- Wrote complex queries for data extraction and analysis

📊 **Advanced Data Analysis with Multiple Tools**  
| Tool       | Purpose |
|-----------|--------|
| **Excel** | Quick filtering, data validation, and preliminary analysis |
| **Python** (Pandas, Matplotlib, Seaborn) | Data cleaning, statistical analysis, and visualization |
| **Power BI** | Interactive dashboard with real-time KPIs for doctors, patients, and finance |

📈 **Extracted Actionable Insights**
- Doctor rating vs. salary correlation
- Payment methods and status analysis (pending, completed, failed, refunded)
- Monthly revenue trends and seasonal patterns
- Department utilization and emergency room capacity

---

## 🗃️ Database Schema

### 🔑 Core Tables
- `Patients` (name, age, gender, visit count)
- `Doctors` (name, specialization, rating, salary)
- `Departments` (name, capacity, staff count)
- `Appointments` (appointment_id, date, notes)
- `Medical_Records` (diagnosis, prescription cost)
- `Payments` (payment_id, amount, status)
- `Visits` (visit_id, visit_date)
- `ChronicDiseases` (disease_id, name)

### 🔗 Junction Tables (Linking)
- `DoctorDepartment`
- `DoctorPhones`, `PatientPhones`
- `DoctorWorkplaces`
- `Department_Equipment`

---

## 📊 Key Insights

### 🏥 Hospital Overview
- 12 departments with a total capacity of **500 beds**
- Current occupancy: **201 patients**
- Daily patient turnover and admission/discharge tracking
- Emergency department utilization monitoring

### 👨‍⚕️ Doctor Performance
- Rating distribution and top performers
- Workload by specialization (Cardiology, Pediatrics, Radiology, Emergency)
- Salary vs. patient rating analysis

### 🧑 Patient Insights
- Demographics: age, gender, city, height, weight
- Visit frequency and most-used departments

### 📅 Appointments & Visits
- Scheduling trends (daily, quarterly)
- Earliest available appointments
- Patient flow analysis

### 💰 Financial Performance
- **Payment Methods**: Credit card, debit card, insurance, cash, online
- **Payment Status**: Pending, completed, failed, refunded
- Total revenue, average transaction value, and completion ratio
- Monthly revenue trends and departmental contributions

---

## 📈 Dashboard & Visualization
Built an **interactive Power BI dashboard** to visualize KPIs and trends, enabling hospital management to:
- Monitor performance in real time
- Optimize resource allocation
- Forecast future needs

---

## 🚀 Tools Used
`SQL` | `Python` | `Power BI` | `Excel` | `Relational Database Design`

---

## 🌟 Impact & Future Vision
Care_Stat empowers healthcare institutions to:
- Make **data-driven decisions**
- Improve **operational efficiency**
- Enhance **patient care quality**
- Ensure **financial sustainability**

**Future Enhancements:**
- Web-based dashboard (React + Flask/Django)
- Predictive analytics using machine learning
- Integration with hospital management systems (HMS)
- Automated reporting and alerts

---

## 🙏 Acknowledgments
We extend our sincere gratitude to:
- **Instant**
- **Engineer Mohamed Abu Obeida**
- **Engineer Mariam**  
for their invaluable support and guidance throughout this project.

---

## 📁 Project Structure (Suggested)
