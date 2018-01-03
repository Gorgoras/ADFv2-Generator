using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    static class LinkedServices
    {

        public static void menu(DataFactoryManagementClient client)
        {
            int opcion = 0;
            while (opcion != 5)
            {
                Console.Write("\n\n************* LINKED SERVICES!! *************\n");
                Console.Write("\nSeleccione una opcion:\n");
                Console.Write("1. Crear SQL Servers On premise\n");
                Console.Write("2. Crear Data Lake\n");
                Console.Write("3. Crear Azure SSIS (test!)\n");
                Console.Write("4. Listar linked services\n");
                Console.Write("5. Volver al menu\n");

                opcion = Int32.Parse(Console.ReadLine());

                switch (opcion)
                {
                    case 1:
                        createSQLServers(client);
                        break;
                    case 2:
                        createDataLake(client);
                        break;
                    case 3:
                        createAzureSSIS(client);
                        break;
                    case 4:
                        listarLinkedServices(client);
                        break;
                }
            }
        }

        private static void createAzureSSIS(DataFactoryManagementClient client)
        {
            var IR = new IntegrationRuntimeReference(DatosGrales.onPremiseIntegrationRuntime);

            var pass = new SecureString("S4nCr1st0b4l");
            LinkedServiceResource SqlServerLinkedServiceSSIS = new LinkedServiceResource(
               new SqlServerLinkedService(
                   new SecureString(@"Data Source=sqlsrvbi00.database.windows.net;Initial Catalog=SSISDB;Integrated Security=False"), null, IR, "Sql que hostea el SSIS", "managerloc", pass));
            client.LinkedServices.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, DatosGrales.linkedServiceSSIS, SqlServerLinkedServiceSSIS);

        }

        private static void listarLinkedServices(DataFactoryManagementClient client)
        {
            var listaLink = client.LinkedServices.ListByFactory(DatosGrales.resourceGroup, DatosGrales.dataFactoryName);
            LinkedServiceResource[] lsr = listaLink.ToArray<LinkedServiceResource>();

            Console.Write("\nLista de linked services:\n");
            for (int i = 0; i < lsr.Length; i++)
            {
                Console.Write("" + (i + 1) + ": " + lsr[i].Name + "\n");
            }
            Console.Write("\n");
        }

        private static void createDataLake(DataFactoryManagementClient client)
        {
            AzureDataLakeStoreLinkedService DataLakeLinkedService = new AzureDataLakeStoreLinkedService();
            DataLakeLinkedService.DataLakeStoreUri = "https://datalakebi00.azuredatalakestore.net/webhdfs/v1";
            DataLakeLinkedService.ServicePrincipalId = DatosGrales.applicationId;
            DataLakeLinkedService.ServicePrincipalKey = new SecureString(DatosGrales.authenticationKey);
            DataLakeLinkedService.ResourceGroupName = DatosGrales.resourceGroup;
            DataLakeLinkedService.SubscriptionId = DatosGrales.subscriptionId;
            DataLakeLinkedService.Tenant = DatosGrales.tenantID;
            LinkedServiceResource DataLakeResource = new LinkedServiceResource(DataLakeLinkedService);
            DataLakeResource.Properties.ConnectVia = new IntegrationRuntimeReference("GatewayEnAzure");

            client.LinkedServices.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "DataLakeStore-LinkedService", DataLakeResource);

        }

        private static void createSQLServers(DataFactoryManagementClient client)
        {
            var IR = new IntegrationRuntimeReference(DatosGrales.onPremiseIntegrationRuntime);

            var pass = new SecureString("LBDq1WEq");
            LinkedServiceResource SqlServerLinkedServiceClaim = new LinkedServiceResource(
               new SqlServerLinkedService(
                   new SecureString(@"Data Source=ROW2K12SQL11;Initial Catalog=ClaimCenter;Integrated Security=True"), null, IR, "Sql Local - ClaimCenter", "SANCRISTOBAL\\_ser_azure_auto", pass));
            client.LinkedServices.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "SqlServerLinkedService-Claim", SqlServerLinkedServiceClaim);

        }


    }
}
