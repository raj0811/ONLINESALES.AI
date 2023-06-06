const fs = require('fs');
const csv = require('csv-parser');

// Define the file paths for the CSV files
const table1Path = 'table1.csv';
const table2Path = 'table2.csv';
const table3Path = 'table3.csv';

// Function to read a CSV file and return a Promise with parsed data
function readCSVFile(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (error) => reject(error));
  });
}

// Function to calculate average salary by department
function calculateAverageSalary(departments, employees, salaries) {
  const avgSalaries = {};
  
  // Calculate total salary and count for each department
  employees.forEach((employee) => {
    const deptID = employee['DEPT ID'];
    if (!avgSalaries[deptID]) {
      avgSalaries[deptID] = {
        totalSalary: 0,
        count: 0
      };
    }
    const empID = employee.ID;
    const employeeSalary = salaries.find((salary) => salary.EMP_ID === empID);
    if (employeeSalary) {
      avgSalaries[deptID].totalSalary += parseFloat(employeeSalary.AMT);
      avgSalaries[deptID].count++;
    }
  });

  // Calculate average salary for each department
  const result = [];
  departments.forEach((department) => {
    const deptID = department.ID;
    const avgSalary = avgSalaries[deptID];
    if (avgSalary && avgSalary.count > 0) {
      const averageMonthlySalary = avgSalary.totalSalary / avgSalary.count;
      result.push({
        DEPT_NAME: department.NAME,
        AVG_MONTHLY_SALARY: averageMonthlySalary.toFixed(2)
      });
    }
  });

  // Sort the result by average salary in descending order
  result.sort((a, b) => b.AVG_MONTHLY_SALARY - a.AVG_MONTHLY_SALARY);

  return result.slice(0, 3); // Return top 3 departments
}

// Main function to generate the report
async function generateReport() {
  try {
    // Read the CSV files
    const table1 = await readCSVFile(table1Path);
    const table2 = await readCSVFile(table2Path);
    const table3 = await readCSVFile(table3Path);

    // Generate the report
    const departments = table1;
    const employees = table2;
    const salaries = table3;
    const report = calculateAverageSalary(departments, employees, salaries);

    // Print the report
    console.log('DEPT_NAME\tAVG_MONTHLY_SALARY (USD)');
    report.forEach((row) => {
      console.log(`${row.DEPT_NAME}\t\t${row.AVG_MONTHLY_SALARY}`);
    });
  } catch (error) {
    console.error('An error occurred:', error);
  }
}

// Call the generateReport function
generateReport();
