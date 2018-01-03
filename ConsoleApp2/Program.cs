using Microsoft.Azure.Management.DataFactory;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    static class Program
    {
        static void Main(string[] args)
        {
            // Authenticate and create a data factory management client
            var client = loguearAzureRm();

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
