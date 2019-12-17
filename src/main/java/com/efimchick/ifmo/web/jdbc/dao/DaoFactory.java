package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {

    private ResultSet getResultSet(String query) {
        try {
            return ConnectionSource.instance().createConnection().createStatement().executeQuery(query);
        } catch (SQLException e) {
            return null;
        }
    }

    private ResultSet getResultSetBySQL(String sqlStatement) {
        try {
            return ConnectionSource.instance().createConnection().createStatement().executeQuery(sqlStatement);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private List<Employee> getEmployeesArrayList(ResultSet resultSet) {
        List<Employee> employees = new ArrayList<>();
        try {
            while (resultSet.next()) {
                employees.add(getEmployee(resultSet));
            }
        } catch (SQLException e) {
            return null;
        }
        return employees;
    }

    private Statement newStatement() throws SQLException {
        return ConnectionSource.instance().createConnection().createStatement();
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                String statement = "SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId();
                return getEmployeesArrayList(getResultSet(statement));
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                String statement = "SELECT * FROM EMPLOYEE WHERE MANAGER = " + employee.getId();
                return getEmployeesArrayList(getResultSet(statement));
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try {
                    String statement = "SELECT * FROM EMPLOYEE WHERE ID = " + Id;
                    ResultSet resultSet = getResultSetBySQL(statement);
                    if (resultSet.next()) {
                        return Optional.of(getEmployee(resultSet));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                String statement = "SELECT * FROM EMPLOYEE";
                return getEmployeesArrayList(getResultSet(statement));
            }

            @Override
            public Employee save(Employee employee) {
                try {
                    String statement = "INSERT INTO EMPLOYEE VALUES ('" +
                                        employee.getId() + "', '" +
                                        employee.getFullName().getFirstName() + "', '" +
                                        employee.getFullName().getLastName() + "', '" +
                                        employee.getFullName().getMiddleName() + "', '" +
                                        employee.getPosition() + "', '" +
                                        employee.getManagerId() + "', '" +
                                        Date.valueOf(employee.getHired()) + "', '" +
                                        employee.getSalary() + "', '" +
                                        employee.getDepartmentId() + "')";
                    newStatement().execute(statement);
                    return employee;
                } catch (SQLException exception){
                    return null;
                }
            }

            @Override
            public void delete(Employee employee) {
                try{
                    String statement = "DELETE FROM EMPLOYEE WHERE ID = " + employee.getId();
                    newStatement().execute(statement);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                try {
                    String statement = "SELECT * FROM DEPARTMENT WHERE ID = " + Id;
                    ResultSet resultSet = getResultSetBySQL(statement);
                    if (resultSet.next()) {
                        return Optional.of(getDepartment(resultSet));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                List<Department> departments = new ArrayList<>();
                try {
                    String statement = "SELECT * FROM DEPARTMENT";
                    ResultSet resultSet = getResultSetBySQL(statement);
                    while (resultSet.next()) {
                        departments.add(getDepartment(resultSet));
                    }
                    return departments;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                try {
                    if (getById(department.getId()).equals(Optional.empty())) {
                        String statement = "INSERT INTO DEPARTMENT VALUES ('" +
                                            department.getId() + "', '" +
                                            department.getName() + "', '" +
                                            department.getLocation() + "')";

                        newStatement().execute(statement);
                    } else {
                        String statement = "UPDATE DEPARTMENT SET NAME = '" +
                                            department.getName() + "', LOCATION = '" +
                                            department.getLocation() + "' WHERE ID = '" +
                                            department.getId() + "'";

                        newStatement().execute(statement);
                    }
                    return department;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public void delete(Department department) {
                try {
                    String statement = "DELETE FROM DEPARTMENT WHERE ID = " + department.getId();
                    newStatement().execute(statement);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private Employee getEmployee(ResultSet resultSet) {
        try {
            return new Employee(
                    new BigInteger(resultSet.getString("ID")),
                    new FullName(
                            resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")
                    ),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    new BigDecimal(resultSet.getString("SALARY")),
                    BigInteger.valueOf(resultSet.getInt("MANAGER")),
                    BigInteger.valueOf(resultSet.getInt("DEPARTMENT"))
            );
        } catch (SQLException e) {
            return null;
        }
    }

    private Department getDepartment(ResultSet resultSet) {
        try {
            return new Department(
                    new BigInteger(resultSet.getString("ID")),
                    resultSet.getString("NAME"),
                    resultSet.getString("LOCATION")
            );
        } catch (SQLException e) {
            return null;
        }
    }

}