package cn.edu.thssdb.type;

import lombok.var;
import javax.annotation.Nullable;

public enum ColumnType {
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  STRING;

  public String text;
  ColumnType(String a){
    this.text=a;
  }

  public static ColumnType fromStr(String a){
    for(var t:ColumnType.values()){
      if(t.text.equals(a)){
        return t;
      }
    }
  return null;
  }
  

}


