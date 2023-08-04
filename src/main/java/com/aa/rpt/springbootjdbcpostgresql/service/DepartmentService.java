package com.aa.rpt.springbootjdbcpostgresql.service;


import com.aa.rpt.springbootjdbcpostgresql.model.Department;

import java.util.List;

public interface DepartmentService {
    Department saveDepartment(Department department);

    List<Department> getAllDepartments();

    List<Department> getDepartments(Object[] params);
}
