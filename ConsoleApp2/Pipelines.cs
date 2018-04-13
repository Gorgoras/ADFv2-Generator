using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    static class Pipelines
    {
        public static void menu(DataFactoryManagementClient client)
        {
            int opcion = 0;
            while (opcion != 13)
            {
                Console.Write("\n\n************* PIPELINES!! *************\n");
                Console.Write("\nSeleccione una opcion:\n");
                Console.Write("1. Crear pipes subida normal\n");
                Console.Write("2. Crear pipes subida con compresion\n");
                Console.Write("3. Crear pipes lake a warehouse\n");
                Console.Write("4. Crear pipes warehouse a lake\n");
                Console.Write("5. Correr todos los pipes a mano\n");
                Console.Write("6. Correr pipes especiales\n");
                Console.Write("7. Correr pipe a mano\n");
                Console.Write("8. Listar pipelines\n");
                Console.Write("9. Crear pipe llamar SSIS (test!)\n");
                Console.Write("10. Corregir pipes (tratar con cuidado!)\n");
                Console.Write("11. Eliminar pipes\n");
                Console.Write("12. Crear pipes dinamismo ETL\n");
                Console.Write("13. Volver al menu\n");

                opcion = Int32.Parse(Console.ReadLine());

                switch (opcion)
                {
                    case 1:
                        crearPipesSubidaNormal(client);
                        break;
                    case 2:
                        crearPipesSubidaConCompresion(client);
                        break;
                    case 3:
                        crearPipesLakeaWarehouse(client);
                        break;
                    case 4:
                        crearPipesWarehouseToLake(client);
                        break;
                    case 5:
                        correrTodosLosPipes(client);
                        break;
                    case 6:
                        correrPipesEspeciales(client);
                        break;
                    case 7:
                        correrPipe(client);
                        break;
                    case 8:
                        listarPipelines(client);
                        break;
                    case 9:
                        crearPipeSSIS(client);
                        break;
                    case 10:
                        corregirPipes(client);
                        break;
                    case 11:
                        eliminarPipes(client);
                        break;
                    case 12:
                        crearPipesDinamismoETL(client);
                        break;
                }
            }
        }

        private static void crearPipesDinamismoETL(DataFactoryManagementClient client)
        {
            
            // Generador de Scripts para claim center
            string[] tablas = new[] { "cc_activity", "cc_check", "cc_checkpayee", "cc_claim", "cc_contact", "cc_coverage", "cc_exchangerate", "cc_exposure", "cc_exposurerpt", "cc_group", "cc_history", "cc_litstatustypeline", "cc_matter", "cc_policy", "cc_riskunit", "cc_transaction", "cc_transactionlineitem", "cc_transactionset", "cc_vehicle", "cctl_approvalstatus", "cctl_costtype", "cctl_coveragesubtype", "cctl_exposurestate", "cctl_exposuretype", "cctl_ext_casequestioned", "cctl_ext_courtvenue", "cctl_ext_damagetype", "cctl_ext_exposurestage", "cctl_historytype", "cctl_losscause", "cctl_matterstatus", "cctl_mattertype", "cctl_transactionstatus" };
            PipelineResource pipe = new PipelineResource(name: "PipeForEachClaim");
            List<Activity> la = new List<Activity>();
            //Foreach activity
            ForEachActivity fea = new ForEachActivity();
            fea.IsSequential = false;
            fea.Name = "ForEachActiv";
            Expression ex = new Expression("@pipeline().parameters.TablasACopiar");
            fea.Items = ex;

            //Copy activity
            List<Activity> la1 = new List<Activity>();
            CopyActivity ca = new CopyActivity();
            ca.EnableStaging = false;
            ca.CloudDataMovementUnits = 5;
            ca.Name = "CopyTabla";

            List<DatasetReference> ldr = new List<DatasetReference>();
            ldr.Add(new DatasetReference("Dataset_Dinamismo_Claim"));
            ca.Inputs = ldr;

            List<DatasetReference> ldo = new List<DatasetReference>();
            ldo.Add(new DatasetReference("Dataset_WHDinamismo_Claim"));
            ca.Outputs = ldo;

            //string consulta = "declare @Tabla varchar(1000);select @Tabla = '@{item()}';declare @vsSQL varchar(8000);declare @vsTableName varchar(50);select @vsTableName = @Tabla;select @vsSQL = '; IF EXISTS (SELECT * FROM '+ @Tabla + ') '+' DROP TABLE '+ @Tabla  + ';'+ ' CREATE TABLE ' + @vsTableName + char(10) + '(' + char(10);select @vsSQL = @vsSQL + ' ' + sc.Name + ' ' +st.Name +case when st.Name in ('varchar','varchar','char','nchar') then '(' + cast(sc.Length as varchar) + ') ' else ' ' end +case when sc.IsNullable = 1 then 'NULL' else 'NOT NULL' end + ',' + char(10) from sysobjects so join syscolumns sc on sc.id = so.id join systypes st on st.xusertype = sc.xusertype where so.name = @vsTableName order by sc.ColID; select substring(@vsSQL,1,len(@vsSQL) - 2) + char(10) + ') ' as QueryCreacion";
            //string consulta = "declare @Tabla as varchar(1000);select @Tabla = '@{item()}';declare @vsSQL varchar(8000);declare @vsTableName varchar(50);select @vsTableName = @Tabla;select @vsSQL = '; IF EXISTS (SELECT * FROM '+ @Tabla + ') '+' DROP TABLE '+ @Tabla  + ';'+ ' CREATE TABLE ' + @vsTableName + char(10) + '(' + char(10);select @vsSQL = @vsSQL + ' ' + column_name + ' ' +data_type +case when data_type in ('varchar','varchar','char','nchar') then '(' + cast(character_maximum_length as varchar) + ') ' else ' ' end +case when is_nullable = 'YES' then 'NULL' else 'NOT NULL' end + ',' + char(10) from information_schema.columns where table_name = @vsTableName order by ordinal_position; if len(@vsSQL) < 4000 select substring(@vsSQL,1,len(@vsSQL) - 2) + char(10) + ') ' as QueryCreacion1, null as QueryCreacion2;else select substring(@vsSQL,1,3999) as QueryCreacion1, SUBSTRING(@vsSQL, 4000, len(@vsSQL) - 2) as QueryCreacion2;";
            string consulta = "declare @Tabla as varchar(1000);select @Tabla = '@{item()}';declare @Schema as varchar(1000);select @Schema = 'landing';declare @vsSQL nvarchar(MAX);declare @vsTableName varchar(50);select @vsTableName = @Tabla;select @vsSQL = '; IF EXISTS (SELECT * FROM information_schema.tables where table_name = '''+ @Tabla + ''' and table_schema = '''+ @Schema +''') '+' DROP TABLE '+ @Tabla  + ';'+ ' CREATE TABLE ' + @vsTableName + char(10) + '(' + char(10);select @vsSQL = @vsSQL + ' ' + column_name + ' ' +data_type +case when data_type in ('varchar','varchar','char','nchar') then '(' + cast(character_maximum_length as varchar) + ') ' else ' ' end +case when is_nullable = 'YES' then 'NULL' else 'NOT NULL' end + ',' + char(10) from information_schema.columns where table_name = @vsTableName order by ordinal_position; set @vsSQL = LEFT(@vsSQL, len(@vsSQL) - 2);if len(@vsSQL) < 3999 select replace(replace(replace(substring(@vsSQL,1,len(@vsSQL)) + char(10) + ') ','-1', '8000'), 'geography', 'varchar(8000)'), 'nvarchar', 'varchar') as QueryCreacion1, null as QueryCreacion2;else select replace(replace(replace(LEFT(@vsSQL,3800),'-1', '8000'), 'geography', 'varchar(8000)'), 'nvarchar', 'varchar') as QueryCreacion1, replace(replace(replace(RIGHT(@vsSQL, Len(@vsSQL) - 3800) + ')','-1', '8000'), 'geography', 'varchar(8000)'), 'nvarchar', 'varchar') as QueryCreacion2;";

            ca.Source = new SqlSource(null, 3, null, consulta, null, null);
            ca.Sink = new SqlSink();


            la1.Add(ca);

            fea.Activities = la1;

            la.Add(fea);
            pipe.Activities = la;
            IDictionary<string, ParameterSpecification> tablasACopiar = new Dictionary<string, ParameterSpecification>();
            tablasACopiar.Add("tablasACopiar", new ParameterSpecification("Array", tablas));
            pipe.Parameters = tablasACopiar;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "PipeForEachClaim", pipe);

            Console.WriteLine("Pipe creado, desea correrlo ahora? (s/n)");
            if(Console.ReadLine() == "s")
            {
                client.Pipelines.CreateRun(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "PipeForEachClaim");
                Console.WriteLine("Run creado! Seguimos creando los de Datastaging.");
            }

            crearPipesDinamismoDataStaging(client);

        }

        private static void crearPipesDinamismoDataStaging(DataFactoryManagementClient client)
        {
            // Generador de Scripts para claim center
            string[] tablas = new[] { "ccst_PagosSAP", "ccst_RAJ" };
            PipelineResource pipe = new PipelineResource(name: "PipeForEachDataStaging");
            List<Activity> la = new List<Activity>();
            //Foreach activity
            ForEachActivity fea = new ForEachActivity();
            fea.IsSequential = false;
            fea.Name = "ForEachActiv";
            Expression ex = new Expression("@pipeline().parameters.TablasACopiar");
            fea.Items = ex;

            //Copy activity
            List<Activity> la1 = new List<Activity>();
            CopyActivity ca = new CopyActivity();
            ca.EnableStaging = false;
            ca.CloudDataMovementUnits = 5;
            ca.Name = "CopyTabla";

            List<DatasetReference> ldr = new List<DatasetReference>();
            ldr.Add(new DatasetReference("Dataset_Dinamismo_Datastaging"));
            ca.Inputs = ldr;

            List<DatasetReference> ldo = new List<DatasetReference>();
            ldo.Add(new DatasetReference("Dataset_WHDinamismo_Claim"));
            ca.Outputs = ldo;

            //string consulta = "declare @Tabla varchar(1000);select @Tabla = '@{item()}';declare @vsSQL varchar(8000);declare @vsTableName varchar(50);select @vsTableName = @Tabla;select @vsSQL = '; IF EXISTS (SELECT * FROM '+ @Tabla + ') '+' DROP TABLE '+ @Tabla  + ';'+ ' CREATE TABLE ' + @vsTableName + char(10) + '(' + char(10);select @vsSQL = @vsSQL + ' ' + sc.Name + ' ' +st.Name +case when st.Name in ('varchar','varchar','char','nchar') then '(' + cast(sc.Length as varchar) + ') ' else ' ' end +case when sc.IsNullable = 1 then 'NULL' else 'NOT NULL' end + ',' + char(10) from sysobjects so join syscolumns sc on sc.id = so.id join systypes st on st.xusertype = sc.xusertype where so.name = @vsTableName order by sc.ColID; select substring(@vsSQL,1,len(@vsSQL) - 2) + char(10) + ') ' as QueryCreacion";
            //string consulta = "declare @Tabla as varchar(1000);select @Tabla = '@{item()}';declare @vsSQL varchar(8000);declare @vsTableName varchar(50);select @vsTableName = @Tabla;select @vsSQL = '; IF EXISTS (SELECT * FROM '+ @Tabla + ') '+' DROP TABLE '+ @Tabla  + ';'+ ' CREATE TABLE ' + @vsTableName + char(10) + '(' + char(10);select @vsSQL = @vsSQL + ' ' + column_name + ' ' +data_type +case when data_type in ('varchar','varchar','char','nchar') then '(' + cast(character_maximum_length as varchar) + ') ' else ' ' end +case when is_nullable = 'YES' then 'NULL' else 'NOT NULL' end + ',' + char(10) from information_schema.columns where table_name = @vsTableName order by ordinal_position; if len(@vsSQL) < 4000 select substring(@vsSQL,1,len(@vsSQL) - 2) + char(10) + ') ' as QueryCreacion1, null as QueryCreacion2;else select substring(@vsSQL,1,3999) as QueryCreacion1, SUBSTRING(@vsSQL, 4000, len(@vsSQL) - 2) as QueryCreacion2;";
            string consulta = "declare @Tabla as varchar(1000);select @Tabla = '@{item()}';declare @Schema as varchar(1000);select @Schema = 'landing';declare @vsSQL nvarchar(MAX);declare @vsTableName varchar(50);select @vsTableName = @Tabla;select @vsSQL = '; IF EXISTS (SELECT * FROM information_schema.tables where table_name = '''+ @Tabla + ''' and table_schema = '''+ @Schema +''') '+' DROP TABLE '+ @Tabla  + ';'+ ' CREATE TABLE ' + @vsTableName + char(10) + '(' + char(10);select @vsSQL = @vsSQL + ' ' + column_name + ' ' +data_type +case when data_type in ('varchar','varchar','char','nchar') then '(' + cast(character_maximum_length as varchar) + ') ' else ' ' end +case when is_nullable = 'YES' then 'NULL' else 'NOT NULL' end + ',' + char(10) from information_schema.columns where table_name = @vsTableName order by ordinal_position; set @vsSQL = LEFT(@vsSQL, len(@vsSQL) - 2);if len(@vsSQL) < 3999 select replace(replace(replace(substring(@vsSQL,1,len(@vsSQL)) + char(10) + ') ','-1', '8000'), 'geography', 'varchar(8000)'), 'nvarchar', 'varchar') as QueryCreacion1, null as QueryCreacion2;else select replace(replace(replace(LEFT(@vsSQL,3800),'-1', '8000'), 'geography', 'varchar(8000)'), 'nvarchar', 'varchar') as QueryCreacion1, replace(replace(replace(RIGHT(@vsSQL, Len(@vsSQL) - 3800) + ')','-1', '8000'), 'geography', 'varchar(8000)'), 'nvarchar', 'varchar') as QueryCreacion2;";


            
            ca.Source = new SqlSource(null, 3, null, consulta, null, null);
            ca.Sink = new SqlSink();


            la1.Add(ca);

            fea.Activities = la1;

            la.Add(fea);
            pipe.Activities = la;
            IDictionary<string, ParameterSpecification> tablasACopiar = new Dictionary<string, ParameterSpecification>();
            tablasACopiar.Add("tablasACopiar", new ParameterSpecification("Array", tablas));
            pipe.Parameters = tablasACopiar;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "PipeForEachDataStaging", pipe);

            Console.WriteLine("Pipe creado, desea correrlo ahora? (s/n)");
            if (Console.ReadLine() == "s")
            {
                client.Pipelines.CreateRun(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "PipeForEachDataStaging");
                Console.WriteLine("Run creado! Volvemos al menu.");
            }
        }

        private static void crearPipesWarehouseToLake(DataFactoryManagementClient client)
        {

        }

        private static void crearPipesLakeaWarehouse(DataFactoryManagementClient client)
        {
            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            PipelineResource pipe;
            string[] tablasWarehouse = DatosGrales.tablasLakeaWarehouse;
            string nombreTablaSinSchema;
            string nombreSinPunto;
            for (int i = 0; i < tablasWarehouse.Length; i++)
            {
                nombreTablaSinSchema = tablasWarehouse[i].Split('.')[1];
                nombreSinPunto = tablasWarehouse[i].Replace('.', '-');
                la = new List<Activity>();
                ca = new CopyActivity();
                ca.Name = "CopyPipeline-Lake-DW-" + nombreTablaSinSchema;
                var sor = new AzureDataLakeStoreSource(recursive: false);


                ca.Source = sor;

                var ware = new SqlDWSink();
                ware.AllowPolyBase = true;
                ware.WriteBatchSize = 1000;



                var poly = new PolybaseSettings();
                poly.RejectValue = 1000;
                poly.RejectType = "value";
                //poly.RejectSampleValue = 0;
                poly.UseTypeDefault = false;



                ware.PolyBaseSettings = poly;
                ware.PreCopyScript = "truncate table landing." + nombreTablaSinSchema;
                ca.Sink = ware;
                ca.EnableStaging = true;
                ca.CloudDataMovementUnits = 0;
                ca.EnableSkipIncompatibleRow = true;

                var stg = new StagingSettings();
                stg.Path = "adfstagingcopydata";

                LinkedServiceReference lsIntermedio = new LinkedServiceReference("StagingStorageLakeToWarehouse");
                stg.LinkedServiceName = lsIntermedio;

                ca.StagingSettings = stg;

                var trans = new TabularTranslator();
                //trans.ColumnMappings = DatosGrales.traerCamposPolybase("cc_history");

                ca.Translator = trans;


                inp = new List<DatasetReference>();
                dr = new DatasetReference("Dataset_Datastaging_DataLakeStore_" + nombreTablaSinSchema);

                inp.Add(dr);
                ca.Inputs = inp;

                outp = new List<DatasetReference>();
                drO = new DatasetReference("Dataset_Warehouse_" + nombreSinPunto);
                outp.Add(drO);
                ca.Outputs = outp;

                la.Add(ca);

                pipe = new PipelineResource();
                pipe.Activities = la;

                if (tablasWarehouse[i] == "landing.ccst_RAJ") {
                    client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Copy-Lake-ADW-" + nombreTablaSinSchema, pipe);
                    Console.Write((i + 1) + ". Pipeline-Copy-Lake-ADW-" + nombreTablaSinSchema + " creado.\n");
                }
            }
        }

        private static void correrPipesEspeciales(DataFactoryManagementClient client)
        {
            //var nombres = DatosGrales.traerTablas(true);
            ////
            //Exception exp = null;
            //for (int i = 0; i < nombres.Length; i++)
            //{
            //    if (esTablaEspecial(nombres[i]))
            //    {
            //        exp = new Exception();
            //        while (exp != null)
            //        {
            //            try
            //            {
            //                client.Pipelines.CreateRun(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombres[i], null);
            //                exp = null;
            //            }
            //            catch (Exception ex) { exp = ex; }
            //        }
            //        Console.Write((i + 1) + ". Run para pipe: Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombres[i] + " creado.\n");
            //    }
            //}

            string pipeName = Console.ReadLine();
            DateTime today = DateTime.Now;
            DateTime lastWeek = DateTime.Now.AddDays(-7);
            PipelineRunFilterParameters prfp = new PipelineRunFilterParameters(lastWeek, today);

            var x = client.PipelineRuns.QueryByFactory("GR_BI_DW_01", "datafactorybi02", prfp);
            var enumerator = x.Value.GetEnumerator();
            PipelineRun pipeRun;
            string runId;
            while (enumerator.MoveNext())
            {
                pipeRun = enumerator.Current;
                if(pipeRun.PipelineName == pipeName)
                {
                    runId = pipeRun.RunId;
                    break;
                }
            }
            

        }

        private static void eliminarPipes(DataFactoryManagementClient client)
        {
            string[] nombreTablas = DatosGrales.traerTablas(true);
            int intentosSinComp = 0;
            int intentosConComp = 0;

            for (int i = 0; i < nombreTablas.Length; i++)
            {
                intentosSinComp = 0;
                intentosConComp = 0;
                while (intentosSinComp < 4)
                {
                    try
                    {
                        client.Pipelines.Delete(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Copy-Claim-" + nombreTablas[i]);
                        Console.Write((i + 1) + ". Sin compresion: Pipeline-Copy-Claim-" + nombreTablas[i] + " eliminado.\n");
                        intentosSinComp = 4;
                    }
                    catch (Exception ex)
                    {
                        //si falla, aumento el contador y espero un segundo antes de intentar de nuevo
                        intentosSinComp++;
                        Console.WriteLine(ex.Message);
                        Thread.Sleep(1000);
                    }
                }

                //while (intentosConComp < 4)
                //{
                //    try
                //    {
                //        client.Pipelines.Delete(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTablas[i]);
                //        Console.Write((i + 1) + ". Con compresion: Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTablas[i] + " eliminado.\n");
                //    }
                //    catch (Exception ex)
                //    {
                //        //si falla, aumento el contador y espero un segundo antes de intentar de nuevo
                //        intentosConComp++;
                //        Thread.Sleep(1000);
                //    }
                //}

            }
        }

        private static void crearPipeSSIS(DataFactoryManagementClient client)
        {
            //string pipeName = "CallSP_SSIS";

            //List<Activity> la = new List<Activity>();
            //CopyActivity ca = new CopyActivity();
            //ca.Name = "CopyParaLlamarSSIS";
            //ca.Source = new SqlSource(null, 3, null, "EXEC sp_LlamarSSIS_Muestra; SELECT 1 as Ayuda");
            //ca.Sink = new SqlSink();

            //var inp = new List<DatasetReference>();
            //var dr = new DatasetReference("DummyDatasetForSSIS");
            //inp.Add(dr);
            //ca.Inputs = inp;

            //var outp = new List<DatasetReference>();
            //var drO = new DatasetReference("DummyDatasetForSSIS");
            //outp.Add(drO);
            //ca.Outputs = outp;

            //la.Add(ca);

            //PipelineResource pipe = new PipelineResource();
            //pipe.Activities = la;

            //client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, pipeName, pipe);
            //Console.Write("Pipeline " + pipeName + "  creado. A continuación se intentará ejecutarlo..\n");

            //Console.WriteLine("\nCreating pipeline run...");
            //CreateRunResponse runResponse = client.Pipelines.CreateRunWithHttpMessagesAsync(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, pipeName).Result.Body;
            //Console.WriteLine("Pipeline run ID: " + runResponse.RunId);

            //Console.WriteLine("Checking pipeline run status...");
            //PipelineRun pipelineRun;
            //while (true)
            //{
            //    pipelineRun = client.PipelineRuns.Get(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, runResponse.RunId);
            //    Console.WriteLine("Status: " + pipelineRun.Status);
            //    if (pipelineRun.Status == "InProgress")
            //        System.Threading.Thread.Sleep(15000);
            //    else
            //        break;
            //}

            PipelineResource pipe = new PipelineResource();
            SqlServerStoredProcedureActivity sp = new SqlServerStoredProcedureActivity("Llamar_SP", "dbo.sp_Call_SSISPack_Siniestro");
            List<Activity> la = new List<Activity>();
            sp.LinkedServiceName = new LinkedServiceReference(DatosGrales.linkedServiceSSIS);
            la.Add(sp);
            pipe.Activities = la;
            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "SSIS_Siniestro", pipe);



            // ESTO ERA PARA LLAMAR A UN SP, NO FUNCIONO ASIQUE VAMOS CON COPY ACTIVITY
            //
            //List<Activity> la = new List<Activity>();
            //var callSP = new SqlServerStoredProcedureActivity();
            //callSP.Name = "CallSP_Activity";
            //callSP.LinkedServiceName = new LinkedServiceReference(DatosGrales.linkedServiceSSIS);
            //callSP.StoredProcedureName = "dbo.sp_LlamarSSIS_Muestra";


            //IDictionary<string, StoredProcedureParameter> dictionary = new Dictionary<string, StoredProcedureParameter>();
            //dictionary.Add("param1", new StoredProcedureParameter(1, "int"));
            //callSP.StoredProcedureParameters = dictionary;
            //callSP.Description = "Activity que llama al SP que llama al paquete de SSIS";
            //IDictionary<string, object> dic = new Dictionary<string, object>();
            //dic.Add("prop1", null);
            //callSP.AdditionalProperties = dic;
            //la.Add(callSP);

            //PipelineResource pipe = new PipelineResource();
            //pipe.Activities = la;
            //pipe.AdditionalProperties = null;

        }

        private static void correrTodosLosPipes(DataFactoryManagementClient client)
        {
            var nombres = DatosGrales.traerTablas(true);
            string nombreBD = DatosGrales.nombreBD;
            if (nombreBD == "ClaimCenter") nombreBD = "Claim";
            //
            Exception exp = null;
            for (int i = 0; i < nombres.Length; i++)
            {
                exp = new Exception();
                while (exp != null)
                {
                    try
                    {
                        client.Pipelines.CreateRun(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-" + nombreBD + "-" + nombres[i], null);
                        exp = null;
                    }
                    catch (Exception ex) { exp = ex; }
                }
                Console.Write((i + 1) + ". Run para pipe: Pipeline-Sql-DataLake-ConCompresion-" + nombreBD + "-" + nombres[i] + " creado.\n");
            }
        }

        private static void listarPipelines(DataFactoryManagementClient client)
        {
            var pl = client.Pipelines.ListByFactory(DatosGrales.resourceGroup, DatosGrales.dataFactoryName);
            PipelineResource[] pipes = pl.ToArray<PipelineResource>();
            Console.Write("\nLista de pipelines: \n");
            for (int i = 0; i < pipes.Length; i++)
            {
                Console.Write("" + (i + 1) + ": " + pipes[i].Name + "\n");
            }
            Console.Write("\n");
        }

        private static void crearPipesSubidaConCompresion(DataFactoryManagementClient client)
        {
            string nombreBD = DatosGrales.nombreBD;
            string[] nombreTablas = DatosGrales.traerTablas(true);
            string[] nombreTablasParaCompresion = DatosGrales.traerTablas(false);
            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            CopyActivity ca2;
            List<ActivityDependency> dep;
            string nombreTablaParaConsulta;
            List<DatasetReference> inp1;
            DatasetReference dr1;
            List<DatasetReference> outp1;
            DatasetReference drO1;
            PipelineResource pipe1;
            string consulta;
            if (nombreBD == "ClaimCenter") nombreBD = "Claim";
            for (int i = 790; i < nombreTablas.Length; i++)
            {
                if (esTablaEspecial(nombreTablas[i]))
                {
                    //no creo nada porque tiene un trato especial
                }
                else
                {
                    nombreTablaParaConsulta = nombreTablas[i].Replace('-', '.');
                    consulta = DatosGrales.queryMagica(nombreTablaParaConsulta, 0);
                    la = new List<Activity>();
                    ca = new CopyActivity();
                    ca.Name = "CA-Compresion-" + nombreTablas[i];
                    ca.Source = new SqlSource(null, 3, null, consulta);
                    ca.Sink = new AzureDataLakeStoreSink();

                    inp = new List<DatasetReference>();
                    //dr = new DatasetReference("Dataset_" + nombreBD + "_" + nombreTablas[i]);
                    dr = new DatasetReference("Dataset_ClaimCenter_" + nombreTablas[i]);
                    inp.Add(dr);
                    ca.Inputs = inp;

                    outp = new List<DatasetReference>();
                    drO = new DatasetReference("Dataset_Descompresion_" + nombreBD + "_DataLakeStore_" + nombreTablasParaCompresion[i]);
                    outp.Add(drO);
                    ca.Outputs = outp;

                    la.Add(ca);

                    ca2 = new CopyActivity();

                    ca2.Name = "CA-Descompresion-" + nombreTablas[i];
                    ca2.Source = new SqlSource();
                    ca2.Sink = new AzureDataLakeStoreSink();
                    string[] condiciones = { "Succeeded" };
                    dep = new List<ActivityDependency>();
                    dep.Add(new ActivityDependency("CA-Compresion-" + nombreTablas[i], condiciones));
                    ca2.DependsOn = dep;


                    inp1 = new List<DatasetReference>();
                    dr1 = new DatasetReference("Dataset_Descompresion_" + nombreBD + "_DataLakeStore_" + nombreTablasParaCompresion[i]);
                    inp1.Add(dr1);
                    ca2.Inputs = inp1;

                    outp1 = new List<DatasetReference>();
                    drO1 = new DatasetReference("Dataset_" + nombreBD + "_DataLakeStore_" + nombreTablasParaCompresion[i]);
                    outp1.Add(drO1);
                    ca2.Outputs = outp1;

                    la.Add(ca2);

                    pipe1 = new PipelineResource();

                    pipe1.Activities = la;

                    client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-" + nombreBD + "-" + nombreTablas[i], pipe1);
                    Console.Write((i + 1) + ". Pipeline-Sql-DataLake-ConCompresion-" + nombreBD + "-" + nombreTablas[i] + " creado.\n");
                }
            }
        }

        private static void crearPipesSubidaNormal(DataFactoryManagementClient client)
        {
            var nombreTablas = DatosGrales.traerTablas(true);
            var nombreSinSchema = DatosGrales.traerTablas(false);

            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            PipelineResource pipe;
            string nombreTablaParaConsulta;
            string consulta;
            string nombreBD = DatosGrales.nombreBD;
            for (int i = 0; i < nombreTablas.Length; i++)
            {
                if (esTablaEspecial(nombreTablas[i]))
                {
                    //no creo nada porque es especial
                }
                else
                {
                    nombreTablaParaConsulta = nombreTablas[i].Replace('-', '.');
                    consulta = DatosGrales.queryMagica(nombreTablaParaConsulta, 10000);
                    la = new List<Activity>();
                    ca = new CopyActivity();
                    ca.Name = "CopyPipeline-Sql-Lake-" + nombreTablas[i];
                    ca.Source = new SqlSource(null, 3, null, consulta);
                    ca.Sink = new SqlSink();

                    inp = new List<DatasetReference>();
                    dr = new DatasetReference("Dataset_" + nombreBD + "_" + nombreTablas[i]);

                    inp.Add(dr);
                    ca.Inputs = inp;

                    outp = new List<DatasetReference>();
                    drO = new DatasetReference("Dataset_" + nombreBD + "_DataLakeStore_" + nombreSinSchema[i]);
                    outp.Add(drO);
                    ca.Outputs = outp;


                    la.Add(ca);


                    pipe = new PipelineResource();
                    pipe.Activities = la;

                    client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Copy-" + nombreBD + "-" + nombreTablas[i], pipe);

                    Console.Write((i + 1) + ". Pipeline-Copy-" + nombreBD + "-" + nombreTablas[i] + " creado.\n");
                }
            }
        }

        private static bool esTablaEspecial(string v)
        {
            bool es = false;
            string nombre = v.Substring(4); //saco el schema y me quedo solo con el nombre de la tabla
            for (int i = 0; i < DatosGrales.tablasEspeciales.Length; i++)
            {
                if (nombre.Equals(DatosGrales.tablasEspeciales[i]))
                {
                    es = true;
                }
            }
            return es;
        }

        public static void correrPipe(DataFactoryManagementClient client)
        {
            Console.Write("\nNombre del pipe a correr:");
            string nombrePipe = Console.ReadLine();

            // Create a pipeline run
            Console.WriteLine("\nCreating pipeline run...");
            CreateRunResponse runResponse = client.Pipelines.CreateRunWithHttpMessagesAsync(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, nombrePipe).Result.Body;
            Console.WriteLine("Pipeline run ID: " + runResponse.RunId);//Pipeline-LlamarSSIS

            /*
            Lista de pipelines: Pipeline-LlamarSSIS
                                Pipeline-Sql-DataLake-Tarea
            */
            // Monitor the pipeline run
            Console.WriteLine("Checking pipeline run status...");
            PipelineRun pipelineRun;
            while (true)
            {
                pipelineRun = client.PipelineRuns.Get(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, runResponse.RunId);
                Console.WriteLine("Status: " + pipelineRun.Status);
                if (pipelineRun.Status == "InProgress")
                    System.Threading.Thread.Sleep(10000);
                else
                    break;
            }

            Console.WriteLine("Pipeline run ID: " + runResponse.RunId + "\n");

        }


        private static void corregirPipes(DataFactoryManagementClient client)
        {
            corregirClaimSinCompresion(client);
            corregirClaimConCompresion(client);
            //corregirAddressSinCompresion(client);
            //corregirAddressConCompresion(client);
            //corregirUserSinCompresion(client);
            //corregirUserConCompresion(client);
            //corregirPrueba(client);

        }

        private static void corregirPrueba(DataFactoryManagementClient client)
        {
            //Creo pipelines que suben mas de una tabla en cada uno.
            int cantidadTablasPorPipe = 1;
            int ayudaRecorrido = 0;
            string[] nombreTablas = DatosGrales.traerTablas(true);
            string[] nombreTablasParaCompresion = DatosGrales.traerTablas(false);
            List<Activity> la = new List<Activity>();
            PipelineReference pipeRef;
            for (int i = 0; i < 1; i++)
            {
                pipeRef = new PipelineReference("Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTablas[i], "Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTablas[i]);
                Dictionary<String, object> diccionarioParams = new Dictionary<String, object>();
                diccionarioParams.Add("Param1", 1);
                ExecutePipelineActivity epa = new ExecutePipelineActivity("ExecPipe-" + nombreTablas[i], pipeRef, diccionarioParams, "Llama al pipe para " + nombreTablas[i], null, diccionarioParams, false);

                la.Add(epa);
            }
            PipelineResource pipe1 = new PipelineResource();
            pipe1.Activities = la;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-ManyActivs-Claim-1", pipe1);
            Console.Write("Mensaje");


        }


        private static void corregirUserConCompresion(DataFactoryManagementClient client)
        {
            string nombreTabla = "dbo-cc_user";
            string nombreTablaParaCompresion = "cc_user";
            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            CopyActivity ca2;
            List<ActivityDependency> dep;
            List<DatasetReference> inp1;
            DatasetReference dr1;
            List<DatasetReference> outp1;
            DatasetReference drO1;
            PipelineResource pipe1;
            string consultaNueva = "select top 10000 [LoadCommandID], [OffsetStatsUpdateTime], [PublicID], [CreateTime], [UserSettingsID], cast([SpatialPointDenorm] as nvarchar(MAX)), [SessionTimeoutSecs], [OrganizationID], [VacationStatus], [Department], [UpdateTime], [ExternalUser], [Language], [ExperienceLevel], [Locale], [ID], [LossType], [AuthorityProfileID], [CreateUserID], [BeanVersion], [NewlyAssignedActivities], [Retired], [DefaultPhoneCountry], [ValidationLevel], [PolicyType], [UpdateUserID], [QuickClaim], [CredentialID], [SystemUserType], [DefaultCountry], [TimeZone], [ContactID], [JobTitle] from cc_user";


            la = new List<Activity>();
            ca = new CopyActivity();
            ca.Name = "CA-Compresion-" + nombreTabla;
            ca.Source = new SqlSource(null, 3, null, consultaNueva);
            ca.Sink = new AzureDataLakeStoreSink();

            inp = new List<DatasetReference>();
            dr = new DatasetReference("Dataset_Claim_" + nombreTabla);
            inp.Add(dr);
            ca.Inputs = inp;

            outp = new List<DatasetReference>();
            drO = new DatasetReference("Dataset_Descompresion_Claim_DataLakeStore_" + nombreTablaParaCompresion);
            outp.Add(drO);
            ca.Outputs = outp;

            la.Add(ca);

            ca2 = new CopyActivity();

            ca2.Name = "CA-Descompresion-" + nombreTabla;
            ca2.Source = new SqlSource();
            ca2.Sink = new AzureDataLakeStoreSink();
            string[] condiciones = { "Succeeded" };
            dep = new List<ActivityDependency>();
            dep.Add(new ActivityDependency("CA-Compresion-" + nombreTabla, condiciones));
            ca2.DependsOn = dep;


            inp1 = new List<DatasetReference>();
            dr1 = new DatasetReference("Dataset_Descompresion_Claim_DataLakeStore_" + nombreTablaParaCompresion);
            inp1.Add(dr1);
            ca2.Inputs = inp1;

            outp1 = new List<DatasetReference>();
            drO1 = new DatasetReference("Dataset_Claim_DataLakeStore_" + nombreTablaParaCompresion);
            outp1.Add(drO1);
            ca2.Outputs = outp1;

            la.Add(ca2);

            pipe1 = new PipelineResource();

            pipe1.Activities = la;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTabla, pipe1);
            Console.Write("Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTabla + " modificado.\n");
        }

        private static void corregirUserSinCompresion(DataFactoryManagementClient client)
        {
            string nombreTabla = "dbo-cc_user";
            string nombreTablaSinEsquema = "cc_user";
            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            PipelineResource pipe;
            string nuevaConsulta = "select top 1000 [LoadCommandID], [OffsetStatsUpdateTime], [PublicID], [CreateTime], [UserSettingsID], cast([SpatialPointDenorm] as nvarchar(MAX)), [SessionTimeoutSecs], [OrganizationID], [VacationStatus], [Department], [UpdateTime], [ExternalUser], [Language], [ExperienceLevel], [Locale], [ID], [LossType], [AuthorityProfileID], [CreateUserID], [BeanVersion], [NewlyAssignedActivities], [Retired], [DefaultPhoneCountry], [ValidationLevel], [PolicyType], [UpdateUserID], [QuickClaim], [CredentialID], [SystemUserType], [DefaultCountry], [TimeZone], [ContactID], [JobTitle] from cc_user";

            la = new List<Activity>();
            ca = new CopyActivity();
            ca.Name = "CopyPipeline-Sql-Lake-" + nombreTabla;
            ca.Source = new SqlSource(null, 3, null, nuevaConsulta);
            ca.Sink = new SqlSink();


            inp = new List<DatasetReference>();
            dr = new DatasetReference("Dataset_Claim_" + nombreTabla);

            inp.Add(dr);
            ca.Inputs = inp;

            outp = new List<DatasetReference>();
            drO = new DatasetReference("Dataset_Claim_DataLakeStore_" + nombreTablaSinEsquema);
            outp.Add(drO);
            ca.Outputs = outp;


            la.Add(ca);


            pipe = new PipelineResource();
            pipe.Activities = la;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Copy-Claim-" + nombreTabla, pipe);
            Console.Write("Pipeline-Copy-Claim-" + nombreTabla + " modificado.\n");
        }

        private static void corregirAddressConCompresion(DataFactoryManagementClient client)
        {
            string nombreTabla = "dbo-cc_address";
            string nombreTablaParaCompresion = "cc_address";
            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            CopyActivity ca2;
            List<ActivityDependency> dep;
            List<DatasetReference> inp1;
            DatasetReference dr1;
            List<DatasetReference> outp1;
            DatasetReference drO1;
            PipelineResource pipe1;
            string consultaNueva = "select top 10000 [LoadCommandID], [PublicID], [BatchGeocode], [CreateTime], [AddressLine1], [AddressLine2], [County], [AddressLine3], cast([SpatialPoint] as nvarchar(MAX)), [CityKanji], [AddressLine2Kanji], [Admin], [State], [AddressBookUID], [UpdateTime], [Country], [ID], [Ext_StreetType], [ExternalLinkID], [CreateUserID], [ValidUntil], [ArchivePartition], [BeanVersion], [CityDenorm], [Retired], [Ext_StreetNumber], [City], [AddressType], [AddressLine1Kanji], [UpdateUserID], [CEDEXBureau], [GeocodeStatus], [CEDEX], [PostalCodeDenorm], [PostalCode], [Subtype], [Description] from cc_address";


            la = new List<Activity>();
            ca = new CopyActivity();
            ca.Name = "CA-Compresion-" + nombreTabla;
            ca.Source = new SqlSource(null, 3, null, consultaNueva);
            ca.Sink = new AzureDataLakeStoreSink();

            inp = new List<DatasetReference>();
            dr = new DatasetReference("Dataset_Claim_" + nombreTabla);
            inp.Add(dr);
            ca.Inputs = inp;

            outp = new List<DatasetReference>();
            drO = new DatasetReference("Dataset_Descompresion_Claim_DataLakeStore_" + nombreTablaParaCompresion);
            outp.Add(drO);
            ca.Outputs = outp;

            la.Add(ca);

            ca2 = new CopyActivity();

            ca2.Name = "CA-Descompresion-" + nombreTabla;
            ca2.Source = new SqlSource();
            ca2.Sink = new AzureDataLakeStoreSink();
            string[] condiciones = { "Succeeded" };
            dep = new List<ActivityDependency>();
            dep.Add(new ActivityDependency("CA-Compresion-" + nombreTabla, condiciones));
            ca2.DependsOn = dep;


            inp1 = new List<DatasetReference>();
            dr1 = new DatasetReference("Dataset_Descompresion_Claim_DataLakeStore_" + nombreTablaParaCompresion);
            inp1.Add(dr1);
            ca2.Inputs = inp1;

            outp1 = new List<DatasetReference>();
            drO1 = new DatasetReference("Dataset_Claim_DataLakeStore_" + nombreTablaParaCompresion);
            outp1.Add(drO1);
            ca2.Outputs = outp1;

            la.Add(ca2);

            pipe1 = new PipelineResource();

            pipe1.Activities = la;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTabla, pipe1);
            Console.Write("Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTabla + " modificado.\n");
        }

        private static void corregirAddressSinCompresion(DataFactoryManagementClient client)
        {
            string nombreTabla = "dbo-cc_address";
            string nombreTablaSinEsquema = "cc_address";
            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            PipelineResource pipe;
            string nuevaConsulta = "select top 1000 [LoadCommandID], [PublicID], [BatchGeocode], [CreateTime], [AddressLine1], [AddressLine2], [County], [AddressLine3], cast([SpatialPoint] as nvarchar(MAX)), [CityKanji], [AddressLine2Kanji], [Admin], [State], [AddressBookUID], [UpdateTime], [Country], [ID], [Ext_StreetType], [ExternalLinkID], [CreateUserID], [ValidUntil], [ArchivePartition], [BeanVersion], [CityDenorm], [Retired], [Ext_StreetNumber], [City], [AddressType], [AddressLine1Kanji], [UpdateUserID], [CEDEXBureau], [GeocodeStatus], [CEDEX], [PostalCodeDenorm], [PostalCode], [Subtype], [Description] from cc_address";

            la = new List<Activity>();
            ca = new CopyActivity();
            ca.Name = "CopyPipeline-Sql-Lake-" + nombreTabla;
            ca.Source = new SqlSource(null, 3, null, nuevaConsulta);
            ca.Sink = new SqlSink();


            inp = new List<DatasetReference>();
            dr = new DatasetReference("Dataset_Claim_" + nombreTabla);

            inp.Add(dr);
            ca.Inputs = inp;

            outp = new List<DatasetReference>();
            drO = new DatasetReference("Dataset_Claim_DataLakeStore_" + nombreTablaSinEsquema);
            outp.Add(drO);
            ca.Outputs = outp;


            la.Add(ca);


            pipe = new PipelineResource();
            pipe.Activities = la;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Copy-Claim-" + nombreTabla, pipe);
            Console.Write("Pipeline-Copy-Claim-" + nombreTabla + " modificado.\n");
        }

        private static void corregirClaimConCompresion(DataFactoryManagementClient client)
        {

        }

        private static void corregirClaimSinCompresion(DataFactoryManagementClient client)
        {
            List<Activity> la;
            CopyActivity ca;
            List<DatasetReference> inp;
            DatasetReference dr;
            List<DatasetReference> outp;
            DatasetReference drO;
            PipelineResource pipe;

            la = new List<Activity>();
            ca = new CopyActivity();
            ca.Name = "CopyPipeline-Lake-DW-" + "cc_history";
            ca.Source = new AzureDataLakeStoreSource(recursive: false);
            var ware = new SqlDWSink();
            ware.AllowPolyBase = true;
            ware.WriteBatchSize = 1000;

            var poly = new PolybaseSettings();
            poly.RejectValue = 0;
            poly.RejectType = "percentage";
            poly.RejectSampleValue = 0;
            poly.UseTypeDefault = true;

            ware.PolyBaseSettings = poly;
            ca.Sink = ware;
            ca.EnableStaging = true;
            ca.CloudDataMovementUnits = 0;
            ca.EnableSkipIncompatibleRow = true;

            var stg = new StagingSettings();
            stg.Path = "adfstagingcopydata";
            LinkedServiceReference lsIntermedio = new LinkedServiceReference("temp_StagingStorage-c0p");
            stg.LinkedServiceName = lsIntermedio;
            ca.StagingSettings = stg;

            var trans = new TabularTranslator();
            //trans.ColumnMappings = DatosGrales.traerCamposPolybase("cc_history");

            ca.Translator = trans;


            inp = new List<DatasetReference>();
            dr = new DatasetReference("Dataset_Descompresion_Claim_DataLakeStore_cc_history");

            inp.Add(dr);
            ca.Inputs = inp;

            outp = new List<DatasetReference>();
            drO = new DatasetReference("Dataset_Warehouse_landing-pruebaDFv2_cc_history");
            outp.Add(drO);
            ca.Outputs = outp;

            la.Add(ca);

            pipe = new PipelineResource();
            pipe.Activities = la;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Copy-Lake-ADW-cc_history", pipe);
            Console.Write("Pipeline-Copy-Lake-ADW-cc_history creado.\n");
        }

    }
}
