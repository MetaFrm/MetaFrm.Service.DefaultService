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
                Factory.Logger.LogError(exception, "DefaultService : {Message}", exception.Message);
                this.serviceTimeout = 60000;
            }

            databaseAdapter = (Database.IAdapter)Factory.CreateInstance("MetaFrm.Database.Adapter");
        }

        Response IService.Request(ServiceData serviceData)
        {
            Response response;

            try
            {
                if (serviceData.ServiceName == null || serviceData.ServiceName != "MetaFrm.Service.DefaultService")
                    throw new Exception("Not MetaFrm.Service.DefaultService");

                if (serviceData.Commands.TryGetValue("GetDatabaseConnectionNames", out _))
                    return GetDatabaseConnectionNames();

                if (serviceData.TransactionScope)
                    using (TransactionScope transactionScope = new(TransactionScopeOption.Required, new TimeSpan(0, 0, 0, 0, serviceTimeout)))
                    {
                        response = this.Execute(serviceData);

                        if (response.Status == Status.OK)
                            transactionScope.Complete();
                    }
                else
                    response = this.Execute(serviceData);

                return response;
            }
            catch (MetaFrmException exception)
            {
                Factory.Logger.LogError(exception, "Request(MetaFrmException) : {Message}", exception.Message);
                return new Response(exception);
            }
            catch (Exception exception)
            {
                Factory.Logger.LogError(exception, "Request(Exception) : {Message}", exception.Message);
                return new Response(exception);
            }
        }

        Response Execute(ServiceData serviceData)
        {
            Response response;
            Dictionary<string, Database.IDatabase> databaseList;
            Dictionary<(string, string), OutPut> outPuts;
            System.Data.DataSet dataSet;
            int tableCount;
            Database.IDatabase database;

            response = new()
            {
                DataSet = new Data.DataSet()
            };

            databaseList = [];
            outPuts = [];
            dataSet = new();//결과 저장 DataSet
            tableCount = 0;

            try
            {
                this.CreateDatabase(databaseList, serviceData);

                foreach (var kvp in serviceData.Commands)
                {
                    Command command = kvp.Value;
                    string commandName = kvp.Key;

                    database = databaseList[command.ConnectionName];

                    database.Command.CommandType = command.CommandType;

                    //파라미터 생성
                    if (database.Command.CommandType != System.Data.CommandType.Text)
                        this.PrepareParameters(database, commandName, command, outPuts);

                    for (int i = 0; i < command.Values.Count; i++)
                    {
                        //파라미터 값 입력
                        if (database.Command.CommandType != System.Data.CommandType.Text)
                            this.SetParameterValues(database, commandName, command, outPuts, i);

                        //프로시져명
                        switch (database.Command.CommandType)
                        {
                            case System.Data.CommandType.Text:
                                database.Command.CommandText = (string?)command.GetValue("Query", i);
                                break;
                            case System.Data.CommandType.StoredProcedure:
                                database.Command.CommandText = command.CommandText;
                                break;
                            case System.Data.CommandType.TableDirect:
                                break;
                        }

                        //실행 및 결과 취합
                        database.DataAdapter.Fill(dataSet);
                        this.ExtractTablesToResponse(response, dataSet, ref tableCount);

                        // Output 파라미터 수집
                        this.CollectOutputParameters(database, commandName, ref outPuts);
                    }

                    database.Command.Parameters.Clear();
                }

                if (response.DataSet.DataTables.Count < 1 && outPuts.Count < 1)
                    response.DataSet = null;
                else if (outPuts.Count > 0)
                    this.AppendOutPutsToResponse(response, outPuts);

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
                        Factory.Logger.LogError(exception, "Execute : {Message}", exception.Message);
                    }
            }

            return response;
        }
        private void PrepareParameters(Database.IDatabase database, string table, Command command, Dictionary<(string, string), OutPut> outPuts)
        {
            foreach (string dataColumn in command.Parameters.Keys)
            {
                Service.Parameter parameter = command.Parameters[dataColumn];
                System.Data.Common.DbParameter dbParameter = database.AddParameter(dataColumn, parameter.DbType, parameter.Size);

                if (parameter.TargetCommandName != null)
                {
                    dbParameter.Direction = System.Data.ParameterDirection.InputOutput;
                    outPuts[(table, dataColumn)] = new OutPut()
                    {
                        SourceTableName = table,
                        SourceParameterName = dataColumn,
                        TargetTableName = parameter.TargetCommandName,
                        TargetParameterName = parameter.TargetParameterName
                    };
                }
                else
                {
                    dbParameter.Direction = System.Data.ParameterDirection.Input;
                }
            }
        }
        private void SetParameterValues(Database.IDatabase database, string table, Command command, Dictionary<(string, string), OutPut> outPuts, int rowIndex)
        {
            foreach (string dataColumn in command.Parameters.Keys)
            {
                if (outPuts.TryGetValue((table, dataColumn), out var outPut))
                    database.Command.Parameters[dataColumn].Value = outPut.Value ?? DBNull.Value;
                else
                    database.Command.Parameters[dataColumn].Value = command.GetValue(dataColumn, rowIndex) ?? DBNull.Value;
            }
        }
        private void ExtractTablesToResponse(Response response, System.Data.DataSet dataSet, ref int tableCount)
        {
            foreach (System.Data.DataTable dataTable in dataSet.Tables)
            {
                dataTable.TableName = tableCount.ToString();

                response.DataSet?.DataTables.Add(new Data.DataTable(dataTable));
                tableCount += 1;
            }

            dataSet.Tables.Clear();
        }
        private void CollectOutputParameters(Database.IDatabase database, string table, ref Dictionary<(string, string), OutPut> outPuts)
        {
            foreach (System.Data.Common.DbParameter dbParameter in database.Command.Parameters)
            {
                if (dbParameter.Direction == System.Data.ParameterDirection.InputOutput)
                {
                    if (outPuts.TryGetValue((table, dbParameter.ParameterName), out var outPut))
                        outPut.Value = dbParameter.Value;
                }
            }
        }
        private void AppendOutPutsToResponse(Response response, Dictionary<(string, string), OutPut> outPuts)
        {
            Data.DataTable outPutTable = new();
            outPutTable.DataColumns.Add(new Data.DataColumn("SourceTableName", "System.String"));
            outPutTable.DataColumns.Add(new Data.DataColumn("SourceParameterName", "System.String"));
            outPutTable.DataColumns.Add(new Data.DataColumn("TargetTableName", "System.String"));
            outPutTable.DataColumns.Add(new Data.DataColumn("TargetParameterName", "System.String"));
            outPutTable.DataColumns.Add(new Data.DataColumn("Value", "System.String"));

            foreach (OutPut dataRow in outPuts.Values)
            {
                Data.DataRow r = new();
                r.Values.Add("SourceTableName", new Data.DataValue(dataRow.SourceTableName));
                r.Values.Add("SourceParameterName", new Data.DataValue(dataRow.SourceParameterName));
                r.Values.Add("TargetTableName", new Data.DataValue(dataRow.TargetTableName));
                r.Values.Add("TargetParameterName", new Data.DataValue(dataRow.TargetParameterName));
                r.Values.Add("Value", new Data.DataValue(dataRow.Value?.ToString()));

                outPutTable.DataRows.Add(r);
            }

            response.DataSet?.DataTables.Add(outPutTable);
        }


        private void CreateDatabase(Dictionary<string, Database.IDatabase> databaseList, ServiceData serviceData)
        {
            Database.IDatabase? database;
            string databaseName;

            //serviceDataSet[i].ConnectionName 으로 database 추가
            for (int i = 0; i < serviceData.Count; i++)
            {
                databaseName = serviceData[i].ConnectionName;
                if (!databaseList.TryGetValue(databaseName, out var value) || value == null)
                {
                    database = this.CreateDatabase(databaseName);

                    if (database != null)
                        databaseList.TryAdd(databaseName, database);
                }
            }
        }

        private Database.IDatabase? CreateDatabase(string connectionName)
        {
            if (connectionName == null)
                return null;

            return this.databaseAdapter.CreateDatabase(connectionName);
        }

        Response GetDatabaseConnectionNames()
        {
            Response response;
            Data.DataTable dataTable;
            string[] databaseConnectionNames;

            response = new();

            dataTable = new("DatabaseNames");

            dataTable.DataColumns.Add(new("DatabaseNames", "System.String"));

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