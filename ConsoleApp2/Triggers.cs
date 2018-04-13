using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    static class Triggers
    {
        public static void menu(DataFactoryManagementClient client)
        {
            int opcion = 0;

            while (opcion != 5)
            {
                Console.Write("\n\n************* TRIGGERS!! *************\n");
                Console.Write("\nSeleccione una opcion:\n");
                Console.Write("1. Crear trigger para pipes con compresion\n");
                Console.Write("2. Play\n");
                Console.Write("3. Stop\n");
                Console.Write("4. Listar triggers\n");
                Console.Write("5. Volver al menu\n");

                opcion = Int32.Parse(Console.ReadLine());

                switch (opcion)
                {
                    case 1:
                        createUpdateTrigger1(client);
                        break;
                    case 2:
                        client.Triggers.StartWithHttpMessagesAsync(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "TriggerCompresion");
                        break;
                    case 3:
                        client.Triggers.StopWithHttpMessagesAsync(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "TriggerCompresion");
                        break;
                    case 4:
                        listarTriggers(client);
                        break;
                }
            }
        }

        public static void createUpdateTrigger(DataFactoryManagementClient client)
        {
            TriggerPipelineReference[] triPipe;
            string[] tablas = DatosGrales.traerTablas(true);
            for (int o = 1; o < (tablas.Length) / 10; o++)
            {
                if (o != Convert.ToInt32(tablas.Length / 10))
                {
                    triPipe = new TriggerPipelineReference[o * 10];//tablas.Length
                }
                else
                {
                    triPipe = new TriggerPipelineReference[tablas.Length];
                }

                PipelineReference pipe;
                for (int i = 0; i < triPipe.Length; i++)
                {
                    if (!tablas[i].Contains("ccst"))
                    {
                        pipe = new PipelineReference("Pipeline-Sql-DataLake-ConCompresion-Claim-" + tablas[i], "Pipeline-Sql-DataLake-ConCompresion-Claim-" + tablas[i]);
                        triPipe[i] = new TriggerPipelineReference(pipe);

                        //Agrego parametro dummy porque en realidad no uso, pero es obligatorio tener.
                        Dictionary<String, object> diccionarioParams = new Dictionary<String, object>();
                        diccionarioParams.Add("Param1", 1);
                        triPipe[i].Parameters = diccionarioParams;
                    }
                }
                DateTime hoy = DateTime.Now.AddDays(-2);
                DateTime fin = hoy.AddDays(15);
                ScheduleTriggerRecurrence str = new ScheduleTriggerRecurrence(null, "Day", 1, hoy, fin);
                str.TimeZone = "UTC";

                ScheduleTrigger schedule = new ScheduleTrigger(str ,null, "Trigger para pipes con compresion", "Stopped", triPipe);

                TriggerResource trig = new TriggerResource(schedule, null, "CompresionSinCCST", "ScheduleTrigger");
                //trig.Proper
                try
                {
                    TriggerResource trig1 = client.Triggers.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "CompresionSinCCST", trig);
                    Console.WriteLine("Trigger creado con " + (o * 10) + " pipelines, a ver si se banca mas?");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Trigger con " + (o * 10) + " pipelines fallo, hacemos otro intento...");
                    o--;
                }
            }
            //var aor = client.Triggers.StartWithHttpMessagesAsync(resourceGroup, dataFactoryName, "Trigger prueba tarea");

        }

        public static void createUpdateTrigger1(DataFactoryManagementClient client)
        {
            TriggerPipelineReference[] triPipe;
            string[] tablas = DatosGrales.traerTablas(true);
            triPipe = new TriggerPipelineReference[5];
            PipelineReference pipe;
            for (int i = 0; i < triPipe.Length; i++)
            {
                if (!tablas[i].Contains("ccst"))
                {
                    pipe = new PipelineReference("Pipeline-Sql-DataLake-ConCompresion-Claim-" + tablas[i], "Pipeline-Sql-DataLake-ConCompresion-Claim-" + tablas[i]);
                    triPipe[i] = new TriggerPipelineReference(pipe);

                    //Agrego parametro dummy porque en realidad no uso, pero es obligatorio tener.
                    Dictionary<String, object> diccionarioParams = new Dictionary<String, object>();
                    diccionarioParams.Add("Param1", 1);
                    triPipe[i].Parameters = diccionarioParams;
                }
            }
            DateTime hoy = DateTime.Now.AddDays(-2);
            DateTime fin = hoy.AddDays(15);
            ScheduleTriggerRecurrence str = new ScheduleTriggerRecurrence(null, "Day", 1, hoy, fin);
            str.TimeZone = "UTC";

            ScheduleTrigger schedule = new ScheduleTrigger(str, null, "Trigger para pipes con compresion", "Stopped", triPipe);

            TriggerResource trig = new TriggerResource(schedule, null, "CompresionSinCCST", "ScheduleTrigger");
            //trig.Proper
            try
            {
                TriggerResource trig1 = client.Triggers.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "CompresionSinCCST1", trig);
                Console.WriteLine("Trigger creado!");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Trigger fallo al crearse =(");

            }
        }
        //var aor = client.Triggers.StartWithHttpMessagesAsync(resourceGroup, dataFactoryName, "Trigger prueba tarea");
    

    public static void listarTriggers(DataFactoryManagementClient client)
    {
        var pl = client.Triggers.ListByFactory(DatosGrales.resourceGroup, DatosGrales.dataFactoryName);
        TriggerResource[] trigs = pl.ToArray<TriggerResource>();
        TriggerResource tAux;
        Console.Write("\nLista de triggers: \n");
        for (int i = 0; i < trigs.Length; i++)
        {
            Console.Write("" + (i + 1) + ": " + trigs[i].Name + "\n");
            Console.Write("\t");
            tAux = client.Triggers.Get(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, trigs[i].Name);
            Console.Write("Trigger:" + tAux.Properties + " ");
            Console.Write("estado: " + trigs[i].Properties.RuntimeState + "\n");
        }
        Console.Write("\n");

    }
}
}
