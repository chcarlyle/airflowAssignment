from __future__ import annotations
import csv, os, random, shutil
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError
from faker import Faker
import matplotlib.pyplot as plt

OUTPUT_DIR = "/opt/airflow/data"
REPORTS_DIR = "/opt/airflow/dags/reports"
TARGET_TABLE = "employees"
SCHEMA = "demo"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="simple_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    # 1. Fetch synthetic datasets
    @task()
    def fetch_persons(n: int = 50) -> str:
        fake = Faker()
        data = [{"firstname": fake.first_name(), "lastname": fake.last_name(), "country": fake.country()} for _ in range(n)]
        path = os.path.join(OUTPUT_DIR, "persons.csv")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Saved {path}")
        return path

    @task()
    def fetch_companies(n: int = 50) -> str:
        fake = Faker()
        data = [{"company": fake.company(), "industry": fake.bs().split()[0].capitalize()} for _ in range(n)]
        path = os.path.join(OUTPUT_DIR, "companies.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Saved {path}")
        return path

    # 2. Merge + simple transformation
    @task()
    def merge_and_transform(persons, companies) -> str:
        persons_data = list(csv.DictReader(open(persons)))
        companies_data = list(csv.DictReader(open(companies)))
        merged = []

        # Define different slopes (salary growth per year of experience)
        dept_slopes = {
        "Engineering": 8000,
        "Sales": 4000,
        "HR": 3000,
        }

        for p, c in zip(persons_data, companies_data):
            department = random.choice(list(dept_slopes.keys()))
            years_exp = random.randint(0, 20)

            base_salary = random.randint(40000, 80000)
            slope = dept_slopes[department]
            salary = base_salary + slope * years_exp

            merged.append({
                "firstname": p["firstname"],
                "lastname": p["lastname"],
                "company": c["company"],
                "department": department,
                "years_exp": years_exp,
                "salary": salary
            })

        path = os.path.join(OUTPUT_DIR, "merged.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=merged[0].keys())
            writer.writeheader()
            writer.writerows(merged)

        print(f"Merged file saved -> {path}")
        return path


    # 3. Load to Postgres
    @task()
    def load_to_db(csv_path, conn_id="Postgres"):
        with open(csv_path, newline="") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            return 0
        cols = rows[0].keys()
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} ({', '.join([c + ' TEXT' for c in cols])});")
        cur.execute(f"DELETE FROM {SCHEMA}.{TARGET_TABLE};")
        insert_sql = f"INSERT INTO {SCHEMA}.{TARGET_TABLE} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))});"
        cur.executemany(insert_sql, [tuple(r.values()) for r in rows])
        conn.commit()
        conn.close()
        print(f"Inserted {len(rows)} rows into {SCHEMA}.{TARGET_TABLE}")
        return len(rows)

    # 4. Multiple linear regression and visualization
    @task()
    def analyze(csv_path):
        import numpy as np
        import matplotlib.pyplot as plt
        from sklearn.linear_model import LinearRegression
        import csv, os

        # Load dataset
        data = list(csv.DictReader(open(csv_path)))
        departments = sorted(set(r["department"] for r in data))
        dept_to_idx = {d: i for i, d in enumerate(departments)}

    # Prepare X (features) and y (target)
        X, y = [], []
        for r in data:
            try:
                dept = dept_to_idx[r["department"]]
                years = float(r["years_exp"])
                salary = float(r["salary"])
            except (KeyError, ValueError):
                continue

        # One-hot encode departments
            one_hot = [1.0 if i == dept else 0.0 for i in range(len(departments))]
            interactions = [years * val for val in one_hot]
            X.append([years] + one_hot + interactions)
            y.append(salary)

        X = np.array(X)
        y = np.array(y)

        # Fit regression model
        model = LinearRegression()
        model.fit(X, y)
        r2 = model.score(X, y)
        print(f"Model R^2: {r2:.3f}")

    # Plot regression fit for each department
        os.makedirs(REPORTS_DIR, exist_ok=True)
        plt.figure(figsize=(8, 5))

        colors = ["blue", "green", "orange"]
        for i, dept in enumerate(departments):
            dept_rows = [r for r in data if r["department"] == dept]
            years = np.array([float(r["years_exp"]) for r in dept_rows])
            salaries = np.array([float(r["salary"]) for r in dept_rows])

            plt.scatter(years, salaries, alpha=0.5, label=f"{dept} data", color=colors[i % len(colors)])

            # Predict salary vs years for this department
            years_line = np.linspace(0, 20, 50)
            one_hot = np.zeros((len(years_line), len(departments)))
            one_hot[:, i] = 1.0
            interx = years_line.reshape(-1, 1) * one_hot
            X_pred = np.column_stack([years_line, one_hot, interx])
            y_pred = model.predict(X_pred)
            plt.plot(years_line, y_pred, color=colors[i % len(colors)], linewidth=2, label=f"{dept} fit")

        plt.title(f"Salary vs Years Experience by Department\nLinear Regression (R²={r2:.2f})")
        plt.xlabel("Years of Experience")
        plt.ylabel("Salary (USD)")
        plt.legend()
        plt.tight_layout()

        plot_path = os.path.join(REPORTS_DIR, "salary_vs_experience_by_dept.png")
        plt.savefig(plot_path)
        plt.close()
        print(f"Plot saved -> {plot_path}")
        return plot_path


    # 5. Clean intermediate files
    @task()
    def clean_folder(folder=OUTPUT_DIR):
        for f in os.listdir(folder):
            os.remove(os.path.join(folder, f))
        print("Cleaned up temp files.")

    persons = fetch_persons()
    companies = fetch_companies()
    merged = merge_and_transform(persons, companies)
    db_load = load_to_db(merged)
    plot = analyze(merged)
    [db_load, plot] >> clean_folder()
