package com.jikexueyuan.cassander_demo;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SecondDemo {

	public static void main(String[] args) {
		
		Cluster cluster = null ;
		Session session = null;
		
		try {
			
			// 定义一个cluster类
			cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			// 需要获取session对象
			session = cluster.connect();
			// 创建键空间
			String createKeySpaceCQL = "create keyspace if not exists testkeyspace1 with replication={'class':'SimpleStrategy', 'replication_factor':1}";
			session.execute(createKeySpaceCQL);
			// 创建列族
			String createTableCQL = "create table if not exists testkeyspace1.student(name varchar primary key, age int)";
			session.execute(createTableCQL);
			// 插入数据
			String insertCQL = "insert into testkeyspace1.student(name, age) values('zhangsan', 20)";
			session.execute(insertCQL);
			// 查询
			String queryCQL = "select * from testkeyspace1.student";
			ResultSet rs = session.execute(queryCQL);
			List<Row> dataList = rs.all();
			for (Row row : dataList) {
				System.out.println("=>name: " + row.getString("name"));
				System.out.println("=>age : " + row.getInt("age"));
			}
			// 修改 
			String updateCQL = "update testkeyspace1.student set age=22 where name='zhangsan'";
			session.execute(updateCQL);
			rs = session.execute(queryCQL);
			dataList = rs.all();
			for (Row row : dataList) {
				System.out.println("=>name: " + row.getString("name"));
				System.out.println("=>age : " + row.getInt("age"));
			}
			//删除数据
			String deleteCQL = "delete from testkeyspace1.student where name='zhangsan'";
			session.execute(deleteCQL);
			rs = session.execute(queryCQL);
			dataList = rs.all();
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
