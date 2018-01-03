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
                        createUpdateTrigger(client);
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
            string[] tablas = DatosGrales.traerTablas(true);
            TriggerPipelineReference[] triPipe = new TriggerPipelineReference[tablas.Length];
            PipelineReference pipe;
            for (int i = 0; i < 10; i++)
            {

                pipe = new PipelineReference("Pipeline-Sql-DataLake-ConCompresion-Claim-" + tablas[i], "Pipeline-Sql-DataLake-ConCompresion-Claim-" + tablas[i]);
                triPipe[i] = new TriggerPipelineReference(pipe);

                //Agrego parametro dummy porque en realidad no uso, pero es obligatorio tener.
                Dictionary<String, object> diccionarioParams = new Dictionary<String, object>();
                diccionarioParams.Add("Param1", 1);
                triPipe[i].Parameters = diccionarioParams;
            }
            DateTime hoy = DateTime.Now.AddDays(-2);
            DateTime fin = hoy.AddDays(15);
            ScheduleTriggerRecurrence str = new ScheduleTriggerRecurrence(null, "Week", 1, hoy, fin);
            str.TimeZone = "UTC";

            ScheduleTrigger schedule = new ScheduleTrigger(null, "Trigger para pipes con compresion", "Started", triPipe, str);

            TriggerResource trig = new TriggerResource(schedule, null, "TriggerCompresion", "ScheduleTrigger");
            //trig.Proper

            client.Triggers.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "TriggerCompresion", trig);
            Console.WriteLine("Trigger creado! Buena suerte con ese schedule :)");
            //var aor = client.Triggers.StartWithHttpMessagesAsync(resourceGroup, dataFactoryName, "Trigger prueba tarea");

        }

        public static void listarTriggers(DataFactoryManagementClient client)
        {
            var pl = client.Triggers.ListByFactory(DatosGrales.resourceGroup, DatosGrales.dataFactoryName);
            TriggerResource[] trigs = pl.ToArray<TriggerResource>();
            Console.Write("\nLista de triggers: \n");
            for (int i = 0; i < trigs.Length; i++)
            {
                Console.Write("" + (i + 1) + ": " + trigs[i].Name + "\n");
                Console.Write("\t");
                Console.Write("Trigger:" + trigs[i].Name + " ");
                Console.Write("estado: " + trigs[i].Properties.RuntimeState + "\n");
            }
            Console.Write("\n");

        }
    }
}
