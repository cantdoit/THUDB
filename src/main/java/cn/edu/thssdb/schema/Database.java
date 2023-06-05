package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  void persist() {
    File databaseDir = new File(name);
    if (!databaseDir.exists()) {
      databaseDir.mkdir(); // Create the directory for the database if it doesn't exist
    }

    for (Table table : tables.values()) {
      File tableFile = new File(databaseDir, table.tableName + ".data");
      try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tableFile))) {
        oos.writeObject(table.serializeRows()); // Serialize and write the table data to disk
      } catch (IOException e) {
        System.err.println("Failed to persist table: " + table.tableName);
        e.printStackTrace();
      }
    }
  }

  public void create(String name, Column[] columns) {
    lock.writeLock().lock();
    try {
      if (tables.containsKey(name)) {
        throw new RuntimeException(
            String.format("Table %s already exists in database %s.", name, this.name));
      }
      Table newTable = new Table(this.name, name, columns);
      tables.put(name, newTable);
      persist(); // Persist the database after creating the table
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void drop(String name) {
    lock.writeLock().lock();
    try {
      if (!tables.containsKey(name)) {
        throw new RuntimeException(
            String.format("Table %s does not exist in database %s.", name, this.name));
      }
      tables.remove(name);
      persist(); // Persist the database after dropping the table
    } finally {
      lock.writeLock().unlock();
    }
  }

  public QueryResult select(QueryTable[] queryTables) {
    lock.readLock().lock();
    try {
      // TODO: Implement logic to perform a SELECT query on the tables within the database
      // This may involve reading the table data, processing the query, and returning the result
      // You can create a new instance of QueryResult based on the queryTables
      return new QueryResult(queryTables);
    } finally {
      lock.readLock().unlock();
    }
  }

  private void recover() {
    File databaseDir = new File(name);
    if (databaseDir.exists() && databaseDir.isDirectory()) {
      for (File tableFile : databaseDir.listFiles()) {
        if (tableFile.isFile() && tableFile.getName().endsWith(".data")) {
          String tableName = tableFile.getName().substring(0, tableFile.getName().lastIndexOf("."));
          try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tableFile))) {
            ArrayList<Row> rows = (ArrayList<Row>) ois.readObject();
            Column[] columns = rows.get(0).getColumns();
            Table table = new Table(this.name, tableName, columns);
            table.deserialize(rows); // Deserialize and restore the table data
            tables.put(tableName, table);
          } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to recover table: " + tableName);
            e.printStackTrace();
          }
        }
      }
    }
  }

  public void quit() {
    lock.writeLock().lock();
    try {
      persist(); // Persist the database before quitting
      // TODO: Add any additional cleanup or resource release operations here
    } finally {
      lock.writeLock().unlock();
    }
  }
}
