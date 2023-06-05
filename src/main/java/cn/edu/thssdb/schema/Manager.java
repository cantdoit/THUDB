package cn.edu.thssdb.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private ArrayList<String> databaseList;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    this.databases = new HashMap<>();
    databaseList = new ArrayList<>();
  }

  public void createDatabase(String name) {
    lock.writeLock().lock();
    try {
      if (databases.containsKey(name)) {
        throw new RuntimeException(String.format("Database %s already exists.", name));
      }
      Database database = new Database(name);
      databases.put(name, database);
      databaseList.add(name);
      System.out.println("Database added: " + name);
      database.persist(); // Persist the newly created database
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void dropDatabase(String name) {
    lock.writeLock().lock();
    try {
      if (!databases.containsKey(name)) {
        throw new RuntimeException(String.format("Database %s does not exist.", name));
      }
      Database database = databases.get(name);
      database.quit();
      databases.remove(name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void switchDatabase() {
    // TODO
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
