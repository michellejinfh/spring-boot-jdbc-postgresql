package com.aa.rpt.springbootjdbcpostgresql.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Department {
    private Long departmentId;
    private String departmentName;
    private String groupName;
}
