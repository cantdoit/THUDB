package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

//调用 Manager
import cn.edu.thssdb.schema.Manager;

//import 相关文件
import cn.edu.thssdb.plan.impl.CreateDatabasePlan;
import cn.edu.thssdb.plan.impl.DropDatabasePlan;
import cn.edu.thssdb.plan.impl.UseDatabasePlan;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement);
    switch (plan.getType()) {
      case CREATE_DB:
        CreateDatabasePlan createDatabasePlan = (CreateDatabasePlan) plan;
        System.out.println("[DEBUG] " + createDatabasePlan);
        //TODO 创建数据库相关逻辑
        String dbName = createDatabasePlan.getDatabaseName();
        //TODO 重要：完善Manager->()
        //Manager.getInstance().createDatabase(dbName);
        break;
        return new ExecuteStatementResp(StatusUtil.success(), false);
      case DROP_DB:
        DropDatabasePlan dropDatabasePlan = (DropDatabasePlan) plan;
        System.out.println("[DEBUG] " + dropDatabasePlan);
        //TODO
        String dbName = dropDatabasePlan.getDatabaseName();
        //Manager.getInstance().dropDatabase(dbName);
        break;
      case USE_DB:
        UseDatabasePlan useDatabasePlan = (UseDatabasePlan) plan;
        System.out.println("[DEBUG] " + useDatabasePlan);
        //TODO
        String dbName = useDatabasePlan.getDatabaseName();
        // Manager.getInstance().useDatabase(dbName);
        // Session session = sessionMap.get(req.getSessionId());
        // session.setCurrentDatabase(dbName);
        break;
      default:
    }
    return null;
  }
}
