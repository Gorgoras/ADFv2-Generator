using Microsoft.Azure.Management.DataFactory;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    static class Program
    {
        static void Main(string[] args)
        {

            // Traigo datos grales del archivo
            cargarDatosGrales();

            // Traigo tablas especiales
            cargarTablasEspeciales();

            // Authenticate and create a data factory management client
            var client = loguearAzureRm();

            // Traer tablas para copiar del lake al warehouse
            traerTablasLakeaWarehouse();

            int opcion = 0;
            
            while (opcion != 6)
            {
                Console.Write("\n************* MENU *************\n");
                Console.Write("\nSeleccione una opcion:\n");
                Console.Write("1. Linked Services\n");
                Console.Write("2. Datasets\n");
                Console.Write("3. Pipelines\n");
                Console.Write("4. Correr pipe\n");
                Console.Write("5. Triggers\n");
                Console.Write("6. Salir\n");

                opcion = Int32.Parse(Console.ReadLine());

                switch (opcion)
                {
                    case 1:
                        menuLinkedServ(client);
                        break;
                    case 2:
                        crearUpdateDatasets(client);
                        break;
                    case 3:
                        crearUpdatePipelines(client);
                        break;
                    case 4:
                        correrPipe(client);
                        break;
                    case 5:
                        crearUpdateTriggers(client);
                        break;
                }       
            }
        }

        private static void traerTablasLakeaWarehouse()
        {
            string[] tabLake = System.IO.File.ReadAllLines(Directory.GetCurrentDirectory() + @"\tablasLakeaWarehouse.txt");
            DatosGrales.tablasLakeaWarehouse = tabLake;
        }

        private static void cargarTablasEspeciales()
        {
            string[] tabEsp = System.IO.File.ReadAllLines(Directory.GetCurrentDirectory() + @"\tablasEspeciales.txt");
            DatosGrales.tablasEspeciales = tabEsp;
        }

        private static void cargarDatosGrales()
        {
            string[] datosGrales = System.IO.File.ReadAllLines(Directory.GetCurrentDirectory() + @"\datos.txt");
            
            DatosGrales.tenantID = datosGrales[0].Split('|')[1];
            DatosGrales.authenticationKey = datosGrales[1].Split('|')[1];
            DatosGrales.applicationId = datosGrales[2].Split('|')[1];
            DatosGrales.subscriptionId = datosGrales[3].Split('|')[1];
            DatosGrales.dataFactoryName = datosGrales[4].Split('|')[1];
            DatosGrales.resourceGroup = datosGrales[5].Split('|')[1];
            DatosGrales.onPremiseIntegrationRuntime = datosGrales[6].Split('|')[1];
            DatosGrales.azureSSISIntegrationRuntime = datosGrales[7].Split('|')[1];
            DatosGrales.azureIntegrationRuntime = datosGrales[8].Split('|')[1];
            DatosGrales.linkedServiceSQLServer = datosGrales[9].Split('|')[1];
            DatosGrales.linkedServiceLake = datosGrales[10].Split('|')[1];
            DatosGrales.linkedServiceSSIS = datosGrales[11].Split('|')[1];
            DatosGrales.BDReferencia = datosGrales[12].Split('|')[1];
            DatosGrales.nombreBD = datosGrales[13].Split('|')[1];
        }

        public static void correrPipe(DataFactoryManagementClient client)
        {
            Pipelines.correrPipe(client);
        }

        private static void crearUpdateTriggers(DataFactoryManagementClient client)
        {
            Triggers.menu(client);
        }


        private static void crearUpdatePipelines(DataFactoryManagementClient client)
        {
            Pipelines.menu(client);
        }

        private static void crearUpdateDatasets(DataFactoryManagementClient client)
        {
            Datasets.menu(client);
        }

        public static DataFactoryManagementClient loguearAzureRm()
        {
            var context = new AuthenticationContext("https://login.windows.net/" + DatosGrales.tenantID);
            ClientCredential cc = new ClientCredential(DatosGrales.applicationId, DatosGrales.authenticationKey);
            AuthenticationResult result = context.AcquireTokenAsync("https://management.azure.com/", cc).Result;
            ServiceClientCredentials cred = new TokenCredentials(result.AccessToken);
            var client = new DataFactoryManagementClient(cred) { SubscriptionId = DatosGrales.subscriptionId }; //HASTA ACA ANDA, SE LOGUEA Y TRAE TOKENS
            return client;
        }

        public static void menuLinkedServ(DataFactoryManagementClient client)
        {
            LinkedServices.menu(client);
        }


    }
}
