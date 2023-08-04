package com.aa.rpt.springbootjdbcpostgresql;

import com.aa.rpt.springbootjdbcpostgresql.model.Department;
import com.aa.rpt.springbootjdbcpostgresql.service.DepartmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@SpringBootApplication
public class SpringBootJdbcPostgresqlApplication implements CommandLineRunner {

    @Autowired
    DepartmentService departmentService;

    public static void main(String[] args) {
        SpringApplication.run(SpringBootJdbcPostgresqlApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        Executor executor = Executors.newFixedThreadPool(5);

        List<CompletableFuture<List<Department>>> futureList = new ArrayList<>();
        for(int i=0; i<10; i++) {
            System.out.println("Added work " + i);

//            Map<String, Object> parms = new HashMap<String, Object>();
//            parms.put("origin", "DFW");
//            parms.put("dest", "ORD");

            CompletableFuture<List<Department>> listCompletableFuture = CompletableFuture
                    .supplyAsync(() -> {
                        System.out.println("Executed by : " + Thread.currentThread().getName() + "...Sleeping 10 millisecond");
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return departmentService.getDepartments(new Object[]{"Finance"});
//                        return departmentService.getAllDepartments();
                    }, executor);

            futureList.add(listCompletableFuture);
        }

        System.out.println("After 10 CompletableFuture are created and supplyAsync() is called");

        for(CompletableFuture<List<Department>> future: futureList) {
            future.get();
            System.out.println("---------get future ");
        }

        System.out.println("Completed");

    }
}
