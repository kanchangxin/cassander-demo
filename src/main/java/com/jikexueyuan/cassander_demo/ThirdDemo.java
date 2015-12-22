package com.jikexueyuan.cassander_demo;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

public class ThirdDemo {

	public static void main(String[] args) {

		
		Cluster cluster = null ;
		Session session = null;
		
		try {
			
			// 定义一个cluster类
			cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			// 需要获取session对象
			session = cluster.connect();
			
			// 新增数据
			
			Insert insert = QueryBuilder.insertInto("testkeyspace1", "student").value("name", "lisi").value("age", 40);
			session.execute(insert);
			
			// 查询数据
			System.out.println("查询数据...");
			Where select = QueryBuilder.select().all().from("testkeyspace1", "student").where(QueryBuilder.eq("name", "lisi"));
			System.out.println(select);
			ResultSet rs = session.execute(select);
			for (Row row : rs.all()) {
				System.out.println("=>name: " + row.getString("name"));
				System.out.println("=>age : " + row.getInt("age"));
			}
			
			// 更新数据
			System.out.println("更新数据...");
			com.datastax.driver.core.querybuilder.Update.Where update = QueryBuilder.update("testkeyspace1", "student").with(QueryBuilder.set("age", 45)).where(QueryBuilder.eq("name", "lisi"));
			System.out.println(update);
			session.execute(update);
			rs = session.execute(select);
			for (Row row : rs.all()) {
				System.out.println("=>name: " + row.getString("name"));
				System.out.println("=>age : " + row.getInt("age"));
			}
			
			// 删除数据
			System.out.println("删除数据...");
			com.datastax.driver.core.querybuilder.Delete.Where delete = QueryBuilder.delete().from("testkeyspace1", "student").where(QueryBuilder.eq("name", "lisi"));
			System.out.println(delete);
			session.execute(delete);
			rs = session.execute(select);
			for (Row row : rs.all()) {
				System.out.println("=>name: " + row.getString("name"));
				System.out.println("=>age : " + row.getInt("age"));
			}			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			// 关闭session和cluster
			session.close();
			cluster.close();
		}

	
	}
	
}
