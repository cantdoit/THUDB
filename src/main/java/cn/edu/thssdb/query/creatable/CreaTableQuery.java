package main.java.cn.edu.thssdb.query.creatable;

import cn.edu.thssdb.query.QueryRequest;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.type.QueryType;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Cells;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class createTableQuery implements QueryRequest {
  private String tableName;
  private List<CreateTableAttr> attrs = new ArrayList<>();
  private String primaryKeyName;

  @Override
  public QueryType getQueryType() {
    return QueryType.CREATE_TABLE;
  }

  @Override
  public QueryResult execute(Database db) {
    QueryResult result = new QueryResult();
    result.setMetaInfo(MetaInfo.fromColumns(null,
      Column.builder().name("affected").build()
    ));
    try {
      Column[] newCols = new Column[this.attrs.size()];
      for (int i = 0;i < this.attrs.size(); ++i) {
        CreateTableAttr attr = this.attrs.get(i);
        newCols[i] = Column.builder()
          .name(attr.getAttrName())
          .notNull(attr.getIsNotNull())
          .primary((primaryKeyName.equals(attr.getAttrName()))? 1 : 0)
          .type(attr.getColumnType())
          .maxLength(attr.getStringTypeLen() == null? 0 : attr.getStringTypeLen())
          .build();
      }
      db.createTable(this.tableName, newCols);
      result.getAttrs().add(
        Cells.fromCell(
          new Cell("1")
        )
      );
    } catch (Exception e) {
      result.getAttrs().add(
        Cells.fromCell(
          new Cell(e.getMessage())
        )
      );
    }

    return result;
  }
}