```sql

        SELECT 
            Region, 
            COUNT(Employee_ID) AS Total_High_Stress_Employees
        FROM employee_data_delta
        WHERE Stress_Level = 'High'
        GROUP BY Region
        ORDER BY Total_High_Stress_Employees DESC
    
```

```sql

        SELECT 
            Region, 
            COUNT(Employee_ID) AS Total_High_Stress_Employees
        FROM delta_table
        WHERE Stress_Level = 'High'
        GROUP BY Region
        ORDER BY Total_High_Stress_Employees DESC
    
```

```sql

        SELECT 
            Region, 
            COUNT(Employee_ID) AS Total_High_Stress_Employees
        FROM dbfs:/FileStore/nd191_assignment11/delta_table
        WHERE Stress_Level = 'High'
        GROUP BY Region
        ORDER BY Total_High_Stress_Employees DESC
    
```

```sql

        SELECT 
            Region, 
            COUNT(Employee_ID) AS Total_High_Stress_Employees
        FROM employee_data_delta
        WHERE Stress_Level = 'High'
        GROUP BY Region
        ORDER BY Total_High_Stress_Employees DESC
    
```

