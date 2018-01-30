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
            while (opcion != 10)
            {
                Console.Write("\n\n************* PIPELINES!! *************\n");
                Console.Write("\nSeleccione una opcion:\n");
                Console.Write("1. Crear pipes subida normal\n");
                Console.Write("2. Crear pipes subida con compresion\n");
                Console.Write("3. Correr todos los pipes a mano\n");
                Console.Write("4. Correr pipes especiales\n");
                Console.Write("5. Correr pipe a mano\n");
                Console.Write("6. Listar pipelines\n");
                Console.Write("7. Crear pipe llamar SSIS (test!)\n");
                Console.Write("8. Corregir pipes (tratar con cuidado!)\n");
                Console.Write("9. Eliminar pipes\n");
                Console.Write("10. Volver al menu\n");

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
                        correrTodosLosPipes(client);
                        break;
                    case 4:
                        correrPipesEspeciales(client);
                        break;
                    case 5:
                        correrPipe(client);
                        break;
                    case 6:
                        listarPipelines(client);
                        break;
                    case 7:
                        crearPipeSSIS(client);
                        break;
                    case 8:
                        corregirPipes(client);
                        break;
                    case 9:
                        eliminarPipes(client);
                        break;
                }
            }
        }

        private static void correrPipesEspeciales(DataFactoryManagementClient client)
        {
            var nombres = DatosGrales.traerTablas(true);
            //
            Exception exp = null;
            for (int i = 0; i < nombres.Length; i++)
            {
                if (esTablaEspecial(nombres[i]))
                {
                    exp = new Exception();
                    while (exp != null)
                    {
                        try
                        {
                            client.Pipelines.CreateRun(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombres[i], null);
                            exp = null;
                        }
                        catch (Exception ex) { exp = ex; }
                    }
                    Console.Write((i + 1) + ". Run para pipe: Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombres[i] + " creado.\n");
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
                        Thread.Sleep(1000);
                    }
                }

                while (intentosConComp < 4)
                {
                    try
                    {
                        client.Pipelines.Delete(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTablas[i]);
                        Console.Write((i + 1) + ". Con compresion: Pipeline-Sql-DataLake-ConCompresion-Claim-" + nombreTablas[i] + " eliminado.\n");
                    }
                    catch (Exception ex)
                    {
                        //si falla, aumento el contador y espero un segundo antes de intentar de nuevo
                        intentosConComp++;
                        Thread.Sleep(1000);
                    }
                }

            }
        }

        private static void crearPipeSSIS(DataFactoryManagementClient client)
        {
            string pipeName = "CallSP_SSIS";

            List<Activity> la = new List<Activity>();
            CopyActivity ca = new CopyActivity();
            ca.Name = "CopyParaLlamarSSIS";
            ca.Source = new SqlSource(null, 3, null, "EXEC sp_LlamarSSIS_Muestra; SELECT 1 as Ayuda");
            ca.Sink = new SqlSink();

            var inp = new List<DatasetReference>();
            var dr = new DatasetReference("DummyDatasetForSSIS");
            inp.Add(dr);
            ca.Inputs = inp;

            var outp = new List<DatasetReference>();
            var drO = new DatasetReference("DummyDatasetForSSIS");
            outp.Add(drO);
            ca.Outputs = outp;

            la.Add(ca);

            PipelineResource pipe = new PipelineResource();
            pipe.Activities = la;

            client.Pipelines.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, pipeName, pipe);
            Console.Write("Pipeline " + pipeName + "  creado. A continuación se intentará ejecutarlo..\n");

            Console.WriteLine("\nCreating pipeline run...");
            CreateRunResponse runResponse = client.Pipelines.CreateRunWithHttpMessagesAsync(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, pipeName).Result.Body;
            Console.WriteLine("Pipeline run ID: " + runResponse.RunId);

            Console.WriteLine("Checking pipeline run status...");
            PipelineRun pipelineRun;
            while (true)
            {
                pipelineRun = client.PipelineRuns.Get(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, runResponse.RunId);
                Console.WriteLine("Status: " + pipelineRun.Status);
                if (pipelineRun.Status == "InProgress")
                    System.Threading.Thread.Sleep(15000);
                else
                    break;
            }

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
            //
            Exception exp = null;
            for (int i = 0; i < nombres.Length; i++)
            {
                exp = new Exception();
                while (exp != null)
                {
                    try
                    {
                        client.Pipelines.CreateRun(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Pipeline-Sql-DataLake-ConCompresion-"+nombreBD+"-" + nombres[i], null);
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
            for (int i = 0; i < nombreTablas.Length; i++)
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
                    dr = new DatasetReference("Dataset_" + nombreBD + "_" + nombreTablas[i]);
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
            //corregirClaimConCompresion(client);
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
            string nombreTabla = "dbo-cc_claim";
            string nombreTablaParaCompresion = "cc_claim";
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
            string consultaNueva = "select top 10000  [ReportedDate], [MainContactType], " +
                "[Ext_Priority], [DateFormGivenToEmp], [PreviousGroupID], [Ext_TemporalClaimNumber], " +
                "[SIULifeCycleState], [SafetyEquipProv], [FlaggedDate], [ExposureBegan], [AssignedByUserID], " +
                "[DateRptdToAgent], [DiagnosticCnsistnt], [Currency], [DateRptdToEmployer], [LitigationStatus], " +
                "[ExposureEnded], [PreviousQueueID], [UpdateTime], [StorageBoxNum], [StateFileNumber], " +
                "[OtherRecovStatus], [DrugsInvolved], [ID], [ISOEnabled], [DateEligibleForArchive], [FlaggedReason]," +
                " [ClaimTier], [ExaminationDate], [CreateUserID], [DeathDate], [Fault], [HowReported]," +
                " [ReinsuranceFlaggedStatus], [LossCause], [BeanVersion], [ReOpenDate], [Progress], [UpdateUserID]," +
                " [ModifiedDutyAvail], [EmploymentDataID], [InsurerSentMPNNotice], [Segment], [LocationOfTheft], " +
                "[StorageType], [EmpQusValidity], [DateCompDcsnDue], [FaultRating], [PublicID], [ClaimantRprtdDate]," +
                " [AssignedGroupID], [LossDate], [SupplementalWorkloadWeight], [ClaimSource], [SIEscalateSIU], " +
                "[Ext_PermanentNumber], [SalvageStatus], [Flagged], [ComputerSecurity], [SafetyEquipUsed]," +
                " [SubrogationStatus], [AssignedQueueID], [StorageLocationState], [EmpSentMPNNotice], " +
                "[EmploymentInjury], [WorkloadUpdated], [ClosedOutcome], [ArchivePartition], [ISOReceiveDate], " +
                "[InsuredPremises], [InjWkrInMPN], [PermissionRequired], [EmployerValidityReason], [Strategy], " +
                "[AssignedUserID], [Ext_isFirstAndFinalWizardMode], [StorageCategory], [PTPinMPN], " +
                "[Ext_LegacyClaimNumber], [LoadCommandID], [StatuteDate], [InjuredOnPremises], [DateFormRetByEmp]," +
                " [InsuredDenormID], [TreatedPatientBfr], [ClaimNumber], [SIEscalateSIUdate], [IncidentReport]," +
                " [PreexDisblty], [Ext_SofiaScore], [HospitalDays], [PoliceDeptInfo], [State], [SIUStatus]," +
                " [FirstNoticeSuit], [Mold], [ReopenedReason], [StateAckNumber], [ReportedByType], [CloseDate], " +
                "[Retired], [StorageDate], [FireDeptInfo], [ValidationLevel], [LOBCode], [WorkloadWeight], " +
                "[Ext_LegacyPrismaNumber], [JurisdictionState], [ConcurrentEmp], [DateRptdToInsured], [ISOStatus]," +
                " [WeatherRelated], [ISOSendDate], [LocationCodeID], [StorageVolumes], [CatastropheID], " +
                "[StorageBarCodeNum], [InjuredRegularJob], [ClaimantDenormID], [LossLocationCode], [Ext_CombSettlement]," +
                " [BenefitsStatusDcsn], [CreateTime], [AccidentType], [PolicyID], [FurtherTreatment], " +
                "[ManifestationDate], [ReinsuranceReportable], [Ext_CreatedByAPI], [PurgeDate], [PreviousUserID], " +
                "[LossType], [ISOKnown], [Ext_LegacyCreateDatetime], [AgencyId], [Ext_Priority_LastDate]," +
                " [HospitalDate], cast([LossLocationSpatialDenorm] as nvarchar(MAX)), [ShowMedicalFirstInfo], " +
                "[DateCompDcsnMade], [LockingColumn], [CurrentConditions], [CoverageInQuestion], [LossLocationID]," +
                " [Weather], [MMIdate], [ClaimWorkCompID], [AssignmentDate], [LargeLossNotificationStatus]," +
                " [Description], [SIScore], [AssignmentStatus], [HazardousWaste], [Ext_ClaimNumberSetDate], " +
                "[Ext_SideatDenunciaNro], [Ext_Observation], [Ext_AssignmentReason], [Ext_CommentAssignment] " +
                "from cc_claim";


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

        private static void corregirClaimSinCompresion(DataFactoryManagementClient client)
        {
            string nombreTablaADW = "landing.pruebaDFv2_cc_history";
            string nombreTablaSinEsquema = "cc_claim";
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
            ca.Source = new AzureDataLakeStoreSource();
            ca.Sink = new SqlDWSink();


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
