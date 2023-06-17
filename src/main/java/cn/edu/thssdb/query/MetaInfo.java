package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

class MetaInfo {

  private String tableName;
  private List<Column> columns;

  MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  int columnFind(String name) {
  return IntStream.range(0, columns.size()).
    filter(i -> columns.get(i).getName().equals(name)).
    findFirst().
    orElse(-1);
    return 0;
  }
}