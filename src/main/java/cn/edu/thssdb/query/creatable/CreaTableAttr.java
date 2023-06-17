package main.java.cn.edu.thssdb.query.creatable;

import cn.edu.thssdb.type.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class CreaTableAttr {
  private String attrName;
  private ColumnType columnType;
  /**
   * 当 `columnType == String` 时设置，表示 String 类型的大小
   */
  @Builder.Default
  private Integer stringTypeLen = 0;
  @Builder.Default
  private Boolean isNotNull = false;
}