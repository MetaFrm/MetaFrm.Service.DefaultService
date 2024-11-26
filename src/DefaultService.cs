using Microsoft.Extensions.Logging;
using System.Transactions;

namespace MetaFrm.Service
{
    /// <summary>
    /// 기본 서비스를 구현합니다.
    /// </summary>
    public class DefaultService : IService
    {
        private readonly int serviceTimeout;
        private readonly Database.IAdapter databaseAdapter;

        /// <summary>
        /// 생성자 입니다.
        /// 서비스 타임 시간(ms)을 가져옵니다.
        /// databaseAdapter를 생성합니다.
        /// </summary>
        public DefaultService()
        {
            try
            {
                this.serviceTimeout = this.GetAttribute("ServiceTimeout").ToInt();
            }
            catch (Exception exception)
            {
                Factory.Logger.LogError(exception, "{Message}", exception.Message);
                this.serviceTimeout = 60000;
            }

            databaseAdapter = (Database.IAdapter)Factory.CreateInstance("MetaFrm.Database.Adapter");
        }

        Response IService.Request(ServiceData serviceData)
        {
            Response response;

            try
            {
                if (serviceData.ServiceName == null || !serviceData.ServiceName.Equals("MetaFrm.Service.DefaultService"))
                    throw new Exception("Not MetaFrm.Service.DefaultService");

                if (serviceData.Commands.TryGetValue("GetDatabaseConnectionNames", out _))
                    return GetDatabaseConnectionNames();

                if (serviceData.TransactionScope)
                    using (TransactionScope transactionScope = new(TransactionScopeOption.Required, new TimeSpan(0, 0, 0, 0, serviceTimeout)))
                    {
                        response = this.Excute(serviceData);

                        if (response.Status == Status.OK)
                            transactionScope.Complete();
                    }
                else
                    response = this.Excute(serviceData);
            }
            catch (MetaFrmException exception)
            {
                Factory.Logger.LogError(exception, "{Message}", exception.Message);
                return new Response(exception);
            }
            catch (Exception exception)
            {
                Factory.Logger.LogError(exception, "{Message}", exception.Message);
                return new Response(exception);
            }

            return response;
        }

        Response Excute(ServiceData serviceData)
        {
            Dictionary<string, Database.IDatabase> databaseList;
            Database.IDatabase database;
            Response? response;
            System.Data.DataSet dataSet;
            System.Data.DataTable dataTable;
            int tableCount;

            List<OutPutTable> outPutTable;

            response = new()
            {
                DataSet = new Data.DataSet()
            };

            databaseList = [];

            try
            {
                this.CreateAndOpenDatabase(databaseList, serviceData);

                outPutTable = [];
                dataSet = new();//결과 저장 DataSet

                tableCount = 0;
                foreach (string table in serviceData.Commands.Keys)
                {
                    database = databaseList[serviceData[table].ConnectionName];

                    database.Command.CommandType = serviceData.Commands[table].CommandType;

                    //파라미터 생성
                    if (database.Command.CommandType != System.Data.CommandType.Text)
                        foreach (string dataColumn in serviceData.Commands[table].Parameters.Keys)
                        {
                            System.Data.Common.DbParameter dbParameter;
                            Database.DbType dbType;

                            dbType = serviceData.Commands[table].Parameters[dataColumn].DbType;

                            dbParameter = database.AddParameter(dataColumn, dbType, serviceData.Commands[table].Parameters[dataColumn].Size);

                            //Target이 있는 파라미터이면
                            if (serviceData.Commands[table].Parameters[dataColumn].TargetCommandName != null)
                            {
                                dbParameter.Direction = System.Data.ParameterDirection.InputOutput;
                                outPutTable.Add(new OutPutTable()
                                {
                                    SourceTableName = table,
                                    SourceParameterName = dataColumn,
                                    TargetTableName = serviceData.Commands[table].Parameters[dataColumn].TargetCommandName,
                                    TargetParameterName = serviceData.Commands[table].Parameters[dataColumn].TargetParameterName
                                });
                            }
                            else
                                dbParameter.Direction = System.Data.ParameterDirection.Input;
                        }

                    for (int i = 0; i < serviceData[table].Values.Count; i++)
                    {
                        //파라미터 값 입력
                        if (database.Command.CommandType != System.Data.CommandType.Text)
                            foreach (string dataColumn in serviceData.Commands[table].Parameters.Keys)
                            {
                                OutPutTable[] dataRows;

                                dataRows = outPutTable.Where(x => x.TargetTableName == table && x.TargetParameterName == dataColumn).ToArray();

                                //_OutPutTable에 있는 항목인지
                                if (dataRows.Length > 0)
                                {
                                    if (dataRows[0].Value == null)
                                        database.Command.Parameters[dataColumn].Value = DBNull.Value;
                                    else
                                        database.Command.Parameters[dataColumn].Value = dataRows[0].Value;
                                }
                                else
                                {
                                    object? value = serviceData[table].GetValue(dataColumn, i);

                                    if (value == null)
                                        database.Command.Parameters[dataColumn].Value = DBNull.Value;
                                    else
                                        database.Command.Parameters[dataColumn].Value = serviceData[table].GetValue(dataColumn, i);
                                }
                            }

                        //프로시져명
                        switch (database.Command.CommandType)
                        {
                            case System.Data.CommandType.Text:
                                database.Command.CommandText = (string?)serviceData[table].GetValue("Query", i);
                                break;
                            case System.Data.CommandType.StoredProcedure:
                                database.Command.CommandText = serviceData[table].CommandText;
                                break;
                            case System.Data.CommandType.TableDirect:
                                break;
                        }

                        database.DataAdapter.Fill(dataSet);

                        while (dataSet.Tables.Count != 0)
                        {
                            dataTable = dataSet.Tables[0];
                            dataTable.TableName = tableCount.ToString();
                            dataSet.Tables.Remove(dataTable);

                            response.DataSet.DataTables.Add(new Data.DataTable(dataTable));

                            tableCount += 1;
                        }

                        foreach (System.Data.Common.DbParameter dbParameter in database.Command.Parameters)
                            if (dbParameter.Direction == System.Data.ParameterDirection.InputOutput)
                            {
                                OutPutTable[] dataRows;

                                dataRows = outPutTable.Where(x => x.SourceTableName == table && x.SourceParameterName == dbParameter.ParameterName).ToArray();

                                if (dataRows.Length > 0)
                                    dataRows[0].Value = dbParameter.Value;

                            }
                    }

                    database.Command.Parameters.Clear();
                }

                if (response.DataSet.DataTables.Count < 1 && outPutTable.Count < 1)
                    response.DataSet = null;
                else
                {
                    if (outPutTable.Count > 0)
                    {
                        Data.DataTable outPutTable1;

                        outPutTable1 = new Data.DataTable();
                        outPutTable1.DataColumns.Add(new Data.DataColumn("SourceTableName", "System.String"));
                        outPutTable1.DataColumns.Add(new Data.DataColumn("SourceParameterName", "System.String"));
                        outPutTable1.DataColumns.Add(new Data.DataColumn("TargetTableName", "System.String"));
                        outPutTable1.DataColumns.Add(new Data.DataColumn("TargetParameterName", "System.String"));
                        outPutTable1.DataColumns.Add(new Data.DataColumn("Value", "System.String"));

                        foreach (OutPutTable dataRow in outPutTable)
                        {
                            Data.DataRow dataRow1 = new();
                            dataRow1.Values.Add("SourceTableName", new Data.DataValue(dataRow.SourceTableName));
                            dataRow1.Values.Add("SourceParameterName", new Data.DataValue(dataRow.SourceParameterName));
                            dataRow1.Values.Add("TargetTableName", new Data.DataValue(dataRow.TargetTableName));
                            dataRow1.Values.Add("TargetParameterName", new Data.DataValue(dataRow.TargetParameterName));
                            dataRow1.Values.Add("Value", new Data.DataValue(dataRow.Value?.ToString()));

                            outPutTable1.DataRows.Add(dataRow1);
                        }

                        response.DataSet.DataTables.Add(outPutTable1);
                    }
                }

                response.Status = Status.OK;
            }
            finally
            {
                foreach (Database.IDatabase database1 in databaseList.Values)
                    try
                    {
                        database1.Close();
                    }
                    catch (Exception exception)
                    {
                        Factory.Logger.LogError(exception, "{Message}", exception.Message);
                    }
            }

            return response;
        }

        private void CreateAndOpenDatabase(Dictionary<string, Database.IDatabase> databaseList, ServiceData serviceData)
        {
            Database.IDatabase? database;
            string databaseName;

            //serviceDataSet[i].ConnectionName 으로 database 추가
            for (int i = 0; i < serviceData.Count; i++)
            {
                databaseName = serviceData[i].ConnectionName;
                if (!databaseList.ContainsKey(databaseName))
                {
                    database = this.CreateAndOpenDatabase(databaseName);

                    if (database != null)
                        databaseList.Add(databaseName, database);
                }
            }
        }

        private Database.IDatabase? CreateAndOpenDatabase(string connectionName)
        {
            if (connectionName == null)
                return null;

            if (this.databaseAdapter == null)
                throw new MetaFrmException("DatabaseAdapter is null");

            return this.databaseAdapter.CreateDatabase(connectionName);
            //database.Connection.Open();
        }

        Response GetDatabaseConnectionNames()
        {
            Response response;
            Data.DataTable dataTable;
            string[] databaseConnectionNames;

            if (this.databaseAdapter == null)
                throw new MetaFrmException("DatabaseAdapter is null");

            response = new();

            dataTable = new("DatabaseNames");

            dataTable.DataColumns.Add(new("DatabaseNames", "System.String"));
            dataTable.DataColumns.Add(new("Database", "System.String"));

            databaseConnectionNames = this.databaseAdapter.ConnectionNames();

            foreach (string tmp in databaseConnectionNames)
            {
                Data.DataRow row = new();
                row.Values.Add("DatabaseNames", new(tmp));

                dataTable.DataRows.Add(row);
            }

            response.DataSet = new();
            response.DataSet.DataTables.Add(dataTable);

            return response;
        }
    }
}