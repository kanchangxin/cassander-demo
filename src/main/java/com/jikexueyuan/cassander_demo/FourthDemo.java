package com.jikexueyuan.cassander_demo;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class FourthDemo {

	public static void main(String[] args) {

		
		Cluster cluster = null ;
		Session session = null;
		
		try {
			
			// 定义一个cluster类
			cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			// 需要获取session对象
			session = cluster.connect();
			
			PreparedStatement statement = session.prepare("insert into testkeyspace1.student(name,age) values(?,?)") ;
			session.execute(statement.bind("wangwu", 26));
			
			// 
			
			String queryCQL = "select * from testkeyspace1.student";
			ResultSet rs = session.execute(queryCQL);
			List<Row> dataList = rs.all();
			for (Row row : dataList) {
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
