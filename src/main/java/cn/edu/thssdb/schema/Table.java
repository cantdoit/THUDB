package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(columns.length);
    for (Column column : columns) {
      this.columns.add(
          new Column(column.name, column.type, column.primary, column.notNull, column.maxLength));
    }
    this.lock = new ReentrantReadWriteLock();
    this.index = new BPlusTree<>();
    this.primaryIndex = -1;

    recover(); // Perform recovery if necessary during initialization
  }

  private void recover() {
    File tableFile = new File(databaseName + File.separator + tableName + ".data");
    if (tableFile.exists()) {
      try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tableFile))) {
        ArrayList<Row> deserializedRows = (ArrayList<Row>) ois.readObject();
        deserialize(deserializedRows);
      } catch (IOException | ClassNotFoundException e) {
        System.err.println("Failed to recover table: " + tableName);
        e.printStackTrace();
      }
    }
  }

  public void insert() {
    // TODO: Implement logic to insert a row into the table
    lock.writeLock().lock();
    try {
      // Insert row and update index

      // After inserting the row, persist the updated table data
      serialize();
    } catch (Exception e) {
      System.err.println("Failed to insert into table: " + tableName);
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void delete() {
    // TODO: Implement logic to delete a row from the table
    lock.writeLock().lock();
    try {
      // Delete row and update index

      // After deleting the row, persist the updated table data
      serialize();
    } catch (Exception e) {
      System.err.println("Failed to delete from table: " + tableName);
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void update() {
    // TODO: Implement logic to update a row in the table
    lock.writeLock().lock();
    try {
      // Update row and update index

      // After updating the row, persist the updated table data
      serialize();
    } catch (Exception e) {
      System.err.println("Failed to update table: " + tableName);
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  void serialize() {
    File tableFile = new File(databaseName + File.separator + tableName + ".data");
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tableFile))) {
      ArrayList<Row> serializedRows = serializeRows();
      oos.writeObject(serializedRows); // Serialize and write the table data to disk
    } catch (IOException e) {
      System.err.println("Failed to persist table: " + tableName);
      e.printStackTrace();
    }
  }

  ArrayList<Row> serializeRows() {
    // Serialize the rows of the table
    ArrayList<Row> serializedRows = new ArrayList<>();
    lock.readLock().lock();
    try {
      BPlusTreeIterator<Entry, Row> iterator = index.iterator();
      while (iterator.hasNext()) {
        Pair<Entry, Row> entryRowPair = iterator.next();
        serializedRows.add(entryRowPair.right);
      }
    } finally {
      lock.readLock().unlock();
    }
    return serializedRows;
  }

  void deserialize(ArrayList<Row> rows) {
    lock.writeLock().lock();
    try {
      index = new BPlusTree<>(); // Create a new BPlusTree instance
      for (Row row : rows) {
        Entry key = row.getEntries().get(primaryIndex);
        index.put(key, row);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
