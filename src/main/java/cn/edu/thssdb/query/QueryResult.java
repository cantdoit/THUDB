package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Cells;
import cn.edu.thssdb.utils.Pair;
import lombok.*;

import java.util.*;
import java.util.stream.IntStream;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class QueryResult {
  private MetaInfo metaInfo;
  private final List<Cells> attrs = new ArrayList<>();

  public static QueryResult fromSingleTable(MetaInfo metaInfo, QueryTable t) {
    Column[] columns = t.getTable().columns;
    //  columns -> metaInfo
    Integer[] mapping = new Integer[metaInfo.getColumns().size()];
    for (int i = 0;i < metaInfo.getColumns().size();++i) {
      Column metaInfoCol = metaInfo.getColumns().get(i);
    }
    // 加入数据
    QueryResult ret = new QueryResult();
    ret.setMetaInfo(metaInfo);
    while (t.hasNext()) {
      Pair<Entry, Row> p = t.next();
      Cells newCells = new Cells();
      for (int i = 0;i < metaInfo.getColumns().size();++i) {
        int targetEntryIdx = mapping[i];
        newCells.getCells().add(
          new Cell(p.right.getEntries().get(targetEntryIdx) == null? "null" : p.right.getEntries().get(targetEntryIdx).value.toString())
        );
      }
      ret.getAttrs().add(newCells);
    }

    return ret;
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    return null;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return null;
  }
}
