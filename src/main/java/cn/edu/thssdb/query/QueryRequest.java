package main.java.cn.edu.thssdb.query;

import cn.edu.thssdb.query.QueryType;
import cn.edu.thssdb.schema.Database;

public interface QueryRequest {
 //获取请求的类型@return 请求类型
  
  QueryType getQueryType();

  QueryResult execute(Database db);
}