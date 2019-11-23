package com.efimchick.ifmo.web.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            Set<Employee> employeeSet = new HashSet<>();
            try {
                while (resultSet.next()) {
                    employeeSet.add(getEmployee(resultSet));
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
            return employeeSet;
        };
    }

    private Employee getEmployee(ResultSet resultSet) throws SQLException {
        try {
            return new Employee(
                    new BigInteger(resultSet.getString("ID")),
                    new FullName(
                            resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    resultSet.getBigDecimal("SALARY"),
                    getManager(resultSet, resultSet.getInt("MANAGER"))
            );
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Employee getManager(ResultSet resultSet, int managerId) throws SQLException {
        Employee manager = null;
        //int managerId = Integer.valueOf(resultSet.getString("MANAGER"));
        int rowNumber = resultSet.getRow();
        resultSet.beforeFirst();

        while (resultSet.next()) {
            if (resultSet.getInt("ID") == managerId) {
                manager = getEmployee(resultSet);
                break;
            }
        }

        resultSet.absolute(rowNumber);
        return manager;
    }
}
