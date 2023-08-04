package com.aa.rpt.springbootjdbcpostgresql.repository;

import com.aa.rpt.springbootjdbcpostgresql.model.Department;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class DepartmentRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${select_all_department}")
    private String selectAllDepartmentQuery;

    @Value("${select_department}")
    private String selectDepartmentQuery;

    public Department save(Department department)
    {
        String sql="insert into department (departmentName, groupName) values ('Finance', 'FI')";
        int rowCount = jdbcTemplate.update(sql);
        System.out.println("A new department is added. rowCount=" + rowCount);

        return null;
    }

    public List<Department> findAll()
    {
//        String sql="select * from HumanResources.Department";
        List<Department> departmentList = jdbcTemplate.query(selectAllDepartmentQuery, BeanPropertyRowMapper.newInstance(Department.class));

        if(departmentList==null)
            System.out.println("departmentList  is null");
        else {
            System.out.println("departmentList size is " + departmentList.size());
//            departmentList.forEach(System.out::println);
        }

        return  departmentList;
    }

    public List<Department> findDepartments(Object[] parms)
    {
        List<Department> departmentList = jdbcTemplate.query(selectDepartmentQuery, parms, BeanPropertyRowMapper.newInstance(Department.class));

        if(departmentList==null)
            System.out.println("departmentList  is null");
        else {
            System.out.println("departmentList size is " + departmentList.size());
//            departmentList.forEach(System.out::println);
        }

        return  departmentList;
    }

    public String getSelectDepartmentQuery() {
        return selectDepartmentQuery;
    }

    public void setSelectDepartmentQuery(String selectDepartmentQuery) {
        this.selectDepartmentQuery = selectDepartmentQuery;
    }
}
