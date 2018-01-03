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
                Console.Write("1. Crear trigger\n");
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
                        client.Triggers.StartWithHttpMessagesAsync(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Trigger prueba tarea");
                        break;
                    case 3:
                        client.Triggers.StopWithHttpMessagesAsync(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Trigger prueba tarea");
                        break;
                    case 4:
                        listarTriggers(client);
                        break;
                }
            }
        }

        public static void createUpdateTrigger(DataFactoryManagementClient client)
        {
            PipelineReference pipe = new PipelineReference("Pipeline-Sql-DataLake-Tarea", "Pipeline-Sql-DataLake-Tarea");
            TriggerPipelineReference[] triPipe = new TriggerPipelineReference[1];
            triPipe[0] = new TriggerPipelineReference(pipe);
            Dictionary<String, object> diccionarioParams = new Dictionary<String, object>();
            diccionarioParams.Add("Param1", 1);
            triPipe[0].Parameters = diccionarioParams;

            DateTime hoy = DateTime.Now;
            DateTime fin = hoy.AddDays(5);
            ScheduleTriggerRecurrence str = new ScheduleTriggerRecurrence(null, "Minute", 3, hoy, fin);
            str.TimeZone = "UTC";

            ScheduleTrigger schedule = new ScheduleTrigger(null, "Trigger de prueba", "Started", triPipe, str);

            TriggerResource trig = new TriggerResource(schedule, null, "Trigger1", "ScheduleTrigger");
            //trig.Proper

            client.Triggers.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Trigger prueba tarea", trig);
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
