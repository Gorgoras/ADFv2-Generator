using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    static class Datasets
    {
        public static void menu(DataFactoryManagementClient client)
        {
            int opcion = 0;
            while (opcion != 8)
            {
                Console.Write("\n\n************* DATASETS!! *************\n");
                Console.Write("\nSeleccione una opcion:\n");
                Console.Write("1. Crear datasets desde sql server\n");
                Console.Write("2. Crear datasets para lake (descompresion)\n");
                Console.Write("3. Crear datasets para lake (compresion)\n");
                Console.Write("4. Crear datasets para warehouse\n");
                Console.Write("5. Crear dummy dataset para SSIS\n");
                Console.Write("6. Corregir datasets\n");
                Console.Write("7. Listar datasets\n");
                Console.Write("8. Volver al menu\n");

                opcion = Int32.Parse(Console.ReadLine());

                switch (opcion)
                {
                    case 1:
                        createDatasetsSQLServer(client);
                        break;
                    case 2:
                        createLakeDatasetsDescomp(client);
                        break;
                    case 3:
                        createLakeDatasetsCompresion(client);
                        break;
                    case 4:
                        createDatasetsWarehouse(client);
                        break;
                    case 5:
                        createDummyDatasetForSSIS(client);
                        break;
                    case 6:
                        corregirDatasets(client);
                        break;
                    case 7:
                        listarDatasets(client);
                        break;
                }
            }
        }

        private static void createDatasetsWarehouse(DataFactoryManagementClient client)
        {
            DatasetResource sqlDatawarehouse;
            string[] tablasWarehouse = DatosGrales.tablasLakeaWarehouse;
            string nombreBD = DatosGrales.nombreBD;
            string nombreSinPunto;
            for (int i = 0; i < tablasWarehouse.Length; i++)
            {
                sqlDatawarehouse = new DatasetResource(
                    new SqlServerTableDataset(
                        new LinkedServiceReference(DatosGrales.linkedServiceWarehouse), tablasWarehouse[i]));
                nombreSinPunto = tablasWarehouse[i].Replace('.', '-');
                client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_Warehouse_" + nombreSinPunto, sqlDatawarehouse);
                Console.Write((i + 1) + ". Dataset_Warehouse_" + nombreSinPunto + " creado.\n");
            }
        }

        private static void corregirDatasets(DataFactoryManagementClient client)
        {
           

        }

        private static void createDummyDatasetForSSIS(DataFactoryManagementClient client)
        {
            DatasetResource dummy;
            LinkedServiceReference lsr = new LinkedServiceReference(DatosGrales.linkedServiceSSIS);
            SqlServerTableDataset sqltd = new SqlServerTableDataset(lsr, "dbo.Ayuda_SSIS");
            dummy = new DatasetResource(sqltd);
            var dsResult = client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "DummyDatasetForSSIS", dummy);

        }

        private static void createLakeDatasetsCompresion(DataFactoryManagementClient client)
        {
            var nombreTablas = DatosGrales.traerTablas(false);
            AzureDataLakeStoreDataset dlsd = new AzureDataLakeStoreDataset();
            TextFormat txtfrm;
            DatasetResource DataLakeDataset;
            string nombreBD = DatosGrales.nombreBD;

            for (int i = 0; i < nombreTablas.Length; i++)
            {
                dlsd.LinkedServiceName = new LinkedServiceReference(DatosGrales.linkedServiceLake);
                dlsd.FolderPath = "Transient Data/" + nombreBD + "/";
                dlsd.FileName = nombreTablas[i] + ".csv.gz";
                dlsd.Compression = new DatasetGZipCompression(null, "Optimal");



                txtfrm = new TextFormat();

                txtfrm.ColumnDelimiter = "|";
                txtfrm.FirstRowAsHeader = true;
                txtfrm.NullValue = "NULL";
                //txtfrm.QuoteChar = "{";


                dlsd.Format = txtfrm;

                DataLakeDataset = new DatasetResource(dlsd);

                client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_Descompresion_" + nombreBD + "_DataLakeStore_" + nombreTablas[i], DataLakeDataset);
                Console.Write((i + 1) + ". Dataset_Descompresion_" + nombreBD + "_DataLakeStore_" + nombreTablas[i] + " creado.\n");
            }
        }

        private static void listarDatasets(DataFactoryManagementClient client)
        {
            var listaDataSet = client.Datasets.ListByFactory(DatosGrales.resourceGroup, DatosGrales.dataFactoryName);
            DatasetResource[] dsr = listaDataSet.ToArray<DatasetResource>();

            Console.Write("\nLista de datasets: \n");
            for (int i = 0; i < dsr.Length; i++)
            {
                Console.Write("" + (i + 1) + ": " + dsr[i].Name + "\n");
            }

            Console.Write("\n");
        }

        private static void createLakeDatasetsDescomp(DataFactoryManagementClient client)
        {
            var nombreTablas = DatosGrales.traerTablas(false);
            AzureDataLakeStoreDataset dlsd1;
            TextFormat txtfrm1;
            DatasetResource DataLakeDataset1;
            for (int i = 0; i < nombreTablas.Length; i++)
            {
                string nombreBD = DatosGrales.nombreBD;
                dlsd1 = new AzureDataLakeStoreDataset();
                dlsd1.LinkedServiceName = new LinkedServiceReference(DatosGrales.linkedServiceLake);
                dlsd1.FolderPath = "Raw Data/" + nombreBD + "/";
                dlsd1.FileName = nombreTablas[i] + ".csv";



                txtfrm1 = new TextFormat();
                txtfrm1.ColumnDelimiter = "|";
                txtfrm1.EncodingName = "Windows-1252"; //default es utf-8, pero no acepta acentos.
                txtfrm1.FirstRowAsHeader = true;
                txtfrm1.NullValue = "NULL";
                dlsd1.Format = txtfrm1;

                DataLakeDataset1 = new DatasetResource(dlsd1);

                client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_" + nombreBD + "_DataLakeStore_" + nombreTablas[i], DataLakeDataset1);
                Console.Write((i + 1) + ". Dataset_" + nombreBD + "_DataLakeStore_" + nombreTablas[i] + " creado.\n");

            }

        }

        private static void createDatasetsSQLServer(DataFactoryManagementClient client)
        {
            var nombreTablas = DatosGrales.traerTablas(true);
            string nombreBD = DatosGrales.nombreBD;
            DatasetResource dsResult;
            DatasetResource sqlDataset;//
            for (int i = 0; i < nombreTablas.Length; i++)
            {
                sqlDataset = new DatasetResource(
                    new SqlServerTableDataset(
                        new LinkedServiceReference(DatosGrales.linkedServiceSQLServer), nombreTablas[i]));
                
                dsResult = client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_" + nombreBD + "_" + nombreTablas[i], sqlDataset);
                Console.Write((i + 1) + ". Dataset_" + nombreBD + "_" + nombreTablas[i] + " creado.\n");
            }
        }



    }
}
