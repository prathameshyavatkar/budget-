pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Budget').getOrCreate()
spark
df = spark.read.csv("C:/Users/prathamesh/OneDrive/Desktop/Big-data folder/Project 3 - (Budget Analysis 2014-2025)", inferSchema=True, header=True)
df.show()
import matplotlib.pyplot as plt

# Years and Defence Budget values
years = list(range(2014, 2026))
defence_budget = [230642.11, 253345.91, 340921.98, 359854.12, 404364.71, 431010.79,
                  471378, 478195.62, 525166.15, 593537.64, 621940.85, 681210]

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(years, defence_budget, marker='o', linestyle='-', color='navy', linewidth=2)
plt.xlabel('Year')
plt.ylabel('Budget (in crores)')
plt.title('Ministry of Defence Budget Over Time (2014–2025)')
plt.xticks(years, rotation=45)
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()

plt.show()
import matplotlib.pyplot as plt

# Years and Health Budget values
years = list(range(2014, 2026))
health_budget = [25133.31, 33278, 38206.35, 48852.51, 54600, 64559.12,
                 67111.8, 73931.77, 86200.65, 89155, 90958.63, 90958.63]

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(years, health_budget, marker='o', linestyle='-', color='seagreen', linewidth=2)
plt.xlabel('Year')
plt.ylabel('Budget (in crores)')
plt.title('Ministry of Health & Family Welfare Budget Over Time (2014–2025)')
plt.xticks(years, rotation=45)
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()


plt.show()
import matplotlib.pyplot as plt

# Years and Agriculture Budget values
years = list(range(2014, 2026))
agri_budget = [12333.64, 12197, 44485.2, 51026, 57600, 138563.97,
               142762.35, 131531.19, 132513.62, 125035.79, 132469.86, 137756]

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(years, agri_budget, marker='o', linestyle='-', color='darkorange', linewidth=2)
plt.xlabel('Year')
plt.ylabel('Budget (in crores)')
plt.title('Ministry of Agriculture & Farmers Welfare Budget Over Time (2014–2025)')
plt.xticks(years, rotation=45)
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()

plt.show()
import matplotlib.pyplot as plt

# Years and Higher Education Budget values
years = list(range(2014, 2026))
higher_ed_budget = [1435.81, 2267, 2461, 2928.65, 1800, 1900,
                    2100, 2663, 40828.35, 44094.62, 47619.77, 47619.77]

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(years, higher_ed_budget, marker='o', linestyle='-', color='mediumpurple', linewidth=2)
plt.xlabel('Year')
plt.ylabel('Budget (in crores)')
plt.title('Higher Education (Medical Ed.) Budget Over Time (2014–2025)')
plt.xticks(years, rotation=45)
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()

plt.show()
import matplotlib.pyplot as plt

# Years and Education Budget values
years = list(range(2014, 2026))
education_budget = [66054.67, 79451, 72394, 79685.95, 85010.29, 94853.64,
                    99311.52, 93224.31, 104277.72, 112899.47, 120627.87, 120627.87]

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(years, education_budget, marker='o', linestyle='-', color='crimson', linewidth=2)
plt.xlabel('Year')
plt.ylabel('Budget (in crores)')
plt.title('Ministry of Education Budget Over Time (2014–2025)')
plt.xticks(years, rotation=45)
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()

plt.show()
import matplotlib.pyplot as plt

# Department names
departments = [
    'Ministry of Defence',
    'Ministry of Health & Family Welfare',
    'Ministry of Agriculture & Farmers Welfare',
    'Higher Education (Medical Ed.)',
    'Ministry of Education (Total)'
]

# Total budget for each department over all years (2014–2025)
totals = [
    sum([230642.11, 253345.91, 340921.98, 359854.12, 404364.71, 431010.79,
         471378, 478195.62, 525166.15, 593537.64, 621940.85, 681210]),
    sum([25133.31, 33278, 38206.35, 48852.51, 54600, 64559.12,
         67111.8, 73931.77, 86200.65, 89155, 90958.63, 90958.63]),
    sum([12333.64, 12197, 44485.2, 51026, 57600, 138563.97,
         142762.35, 131531.19, 132513.62, 125035.79, 132469.86, 137756]),
    sum([1435.81, 2267, 2461, 2928.65, 1800, 1900,
         2100, 2663, 40828.35, 44094.62, 47619.77, 47619.77]),
    sum([66054.67, 79451, 72394, 79685.95, 85010.29, 94853.64,
         99311.52, 93224.31, 104277.72, 112899.47, 120627.87, 120627.87])
]

# Pie chart
plt.figure(figsize=(10, 10))
plt.pie(totals, labels=departments, autopct='%1.1f%%', startangle=140,
        colors=plt.cm.Paired(range(len(departments))))
plt.title('Total Budget Distribution by Department (2014–2025)')
plt.axis('equal')  # Ensures the pie is a perfect circle

plt.show()
