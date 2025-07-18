# 🎬 Movie Recommendation Pipeline using TMDB API

โปรเจคนี้เป็นระบบ **End-to-End Pipeline** สำหรับการดึงข้อมูลหนังจาก [TMDB API](https://www.themoviedb.org/documentation/api) แล้วทำการจัดเก็บใน **PostgreSQL Database** จากนั้นนำข้อมูลไปใช้เพื่อ **Train โมเดลแนะนำหนัง (Movie Recommendation System)** โดยอัตโนมัติ

---

## 📌 คุณสมบัติหลัก (Features)

- ดึงข้อมูลจาก TMDB API (Movies, Genres)
- ทำ ETL (Extract, Transform, Load) ด้วย Apache Airflow
- จัดเก็บข้อมูลใน PostgreSQL Database
- ใช้ Python + Scikit-learn สร้างโมเดล Content-Based Filtering System
- บันทึกโมเดลหรือเรียกใช้จาก Database โดยตรง
- (Optional) บริการ API แนะนำหนังด้วย FastAPI

---


## 📱 ตัวอย่าง UI 
![Movie Recommendation Web or API Demo](images/image4.jpg)
![Movie Recommendation Web or API Demo](images/image2.jpg)
![Movie Recommendation Web or API Demo](images/image1.jpg)
![Movie Recommendation Web or API Demo](images/image5.jpg)

*ตัวอย่างการแสดงผลการแนะนำหนังผ่านหน้าเว็บหรือ API (เช่น FastAPI)*

---
