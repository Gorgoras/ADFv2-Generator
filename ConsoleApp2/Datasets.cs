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
            while (opcion != 9)
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
                Console.Write("8. Crear datasets para dinamismo de ETL\n");
                Console.Write("9. Volver al menu\n");

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
                    case 8:
                        crearDatasetsDinamismo(client);
                        break;
                }
            }
        }

        private static void crearDatasetsDinamismo(DataFactoryManagementClient client)
        {   ///SQL on premise para claim center
            DatasetResource sqlDataset = new DatasetResource(
                    new SqlServerTableDataset(
                        new LinkedServiceReference("SqlServerLinkedServiceClaim"), "DinamismoClaimCenter"));

            client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_Dinamismo_Claim", sqlDataset);
            Console.WriteLine("Creado dataset para dinamismo en claim center on premise.");

            ///SQL en warehouse para claim center (todos en realidad)
            DatasetResource sqlDataset1 = new DatasetResource(
                    new SqlServerTableDataset(
                        new LinkedServiceReference("SqlServerLinkedServiceWarehouse"), "dbo.ScriptsCreacion"));

            client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_WHDinamismo_Claim", sqlDataset1);
            Console.WriteLine("Creado dataset para dinamismo en claim center en warehouse.");

            ///SQL on premise para Datastaging
            DatasetResource sqlDataset2 = new DatasetResource(
                    new SqlServerTableDataset(
                        new LinkedServiceReference("SqlServerLinkedServiceDataStaging"), "DinamismoDatastaging"));

            client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_Dinamismo_Datastaging", sqlDataset2);
            Console.WriteLine("Creado dataset para dinamismo en datastaging on premise.");
            

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
                DatasetDataElement[] estructura = armarEstructuraDataset(tablasWarehouse[i].Split('.')[1]);

                sqlDatawarehouse.Properties.Structure = estructura;
                

                nombreSinPunto = tablasWarehouse[i].Replace('.', '-');
                if (tablasWarehouse[i] == "landing.ccst_RAJ")
                {
                    client.Datasets.CreateOrUpdate(DatosGrales.resourceGroup, DatosGrales.dataFactoryName, "Dataset_Warehouse_" + nombreSinPunto, sqlDatawarehouse);
                    Console.Write((i + 1) + ". Dataset_Warehouse_" + nombreSinPunto + " creado.\n");
                }
            }
        }

        private static DatasetDataElement[] armarEstructuraDataset(string nombreTabla)
        {
            //Traigo cuantas columnas son
            SqlConnection con = new SqlConnection(DatosGrales.warehouseConnectionString);
            String query = "select COUNT(*) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='" + nombreTabla + "'";
            SqlCommand cmd = new SqlCommand(query, con);
            con.Open();
            int cant = (int)cmd.ExecuteScalar();
            con.Close();

            DatasetDataElement[] ret = new DatasetDataElement[cant];
            query = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='" + nombreTabla + "'";

            cmd = new SqlCommand(query, con);
            con.Open();
            SqlDataReader varReader = cmd.ExecuteReader();
            int o = 0;
            while (varReader.Read())
            {
                ret[o] = new DatasetDataElement((string)varReader.GetValue(0), "String");
                o++;
            }
            con.Close();

            return ret;

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

            for (int i = 1190; i < nombreTablas.Length; i++)
            {
                dlsd.LinkedServiceName = new LinkedServiceReference(DatosGrales.linkedServiceLake);
                dlsd.FolderPath = "Transient Data/" + nombreBD + "/";
                dlsd.FileName = nombreTablas[i] + ".csv.gz";
                dlsd.Compression = new DatasetGZipCompression(null, "Optimal");
                
                txtfrm = new TextFormat();

                txtfrm.ColumnDelimiter = "|";
                txtfrm.EncodingName = "Windows-1252";
                txtfrm.FirstRowAsHeader = true;
                //txtfrm.NullValue = "";
                txtfrm.TreatEmptyAsNull = true;
                //txtfrm.QuoteChar = "{";


                dlsd.Format = txtfrm;

                DataLakeDataset = new DatasetResource(dlsd);

                if (nombreBD == "ClaimCenter") nombreBD = "Claim";

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
            for (int i = 1050; i < nombreTablas.Length; i++)
            {
                string nombreBD = DatosGrales.nombreBD;
                dlsd1 = new AzureDataLakeStoreDataset();
                dlsd1.LinkedServiceName = new LinkedServiceReference(DatosGrales.linkedServiceLake);
                dlsd1.FolderPath = "Raw Data/"+ nombreBD + "/";
                dlsd1.FileName = nombreTablas[i] + ".csv";



                txtfrm1 = new TextFormat();
                txtfrm1.ColumnDelimiter = "|";
                txtfrm1.EncodingName = "Windows-1252"; //default es utf-8, pero no acepta acentos.
                txtfrm1.FirstRowAsHeader = true;
                txtfrm1.TreatEmptyAsNull = true;
                //txtfrm1.NullValue = "";
                dlsd1.Format = txtfrm1;

                DataLakeDataset1 = new DatasetResource(dlsd1);
                
                if (nombreBD == "ClaimCenter") nombreBD = "Claim";

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
