package com.efimchick.ifmo.web.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            try {
                Set<Employee> employeeSet = new LinkedHashSet<>();

                while (resultSet.next()) {
                    employeeSet.add(getEmployee(resultSet));
                }
                return employeeSet;
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        };
    }

    private Employee getEmployee(ResultSet resultSet) throws SQLException {
        try {
            Employee manager = null;

            Integer managerId = Integer.valueOf(resultSet.getString("MANAGER"));
            Integer rowNumber = Integer.valueOf(String.valueOf(resultSet.getRow()));

            if (managerId == null) {
                return null;
            }

            resultSet.beforeFirst();

            while (resultSet.next()) {
                if (managerId.equals(Integer.valueOf(resultSet.getString("ID")))) {
                    manager = getEmployee(resultSet);
                    break;
                }
            }
            resultSet.absolute(rowNumber);

            return new Employee(
                    new BigInteger(resultSet.getString("ID")),
                    new FullName(
                            resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    resultSet.getBigDecimal("SALARY"),
                    manager
            );
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
