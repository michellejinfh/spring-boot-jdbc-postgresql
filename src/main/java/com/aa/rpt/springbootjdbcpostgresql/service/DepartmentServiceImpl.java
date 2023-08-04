package com.aa.rpt.springbootjdbcpostgresql.service;

import com.aa.rpt.springbootjdbcpostgresql.model.Department;
import com.aa.rpt.springbootjdbcpostgresql.repository.DepartmentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DepartmentServiceImpl implements DepartmentService{

    @Autowired
    private DepartmentRepository departmentRepository;

    @Override
    public Department saveDepartment(Department department) {
        return departmentRepository.save(department);
    }

    @Override
    public List<Department> getAllDepartments() {
        return departmentRepository.findAll();
    }

    @Override
    public List<Department> getDepartments(Object[] params) {
        return departmentRepository.findDepartments(params);
    }
}
