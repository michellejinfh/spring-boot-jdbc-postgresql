package com.aa.rpt.springbootjdbcpostgresql.controller;

import com.aa.rpt.springbootjdbcpostgresql.model.Department;
import com.aa.rpt.springbootjdbcpostgresql.service.DepartmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

//@RestController
public class DepartmentController {

//    @Autowired
//    private DepartmentService departmentService;
//
//    private final Logger logger = LoggerFactory.getLogger(DepartmentController.class);
//
//    @PostMapping("/departments")
//    public Department saveDepartment(@RequestBody Department department)
//    {
//        logger.info("Inside saveDepartment()....");
//        return departmentService.saveDepartment(department);
//    }
//
//    @GetMapping("/departments")
//    public List<Department> getAllDepartments()
//    {
//        logger.info("Inside getAllDepartments()....");
//        return departmentService.getAllDepartments();
//    }
}
