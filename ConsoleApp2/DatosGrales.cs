using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    static class DatosGrales
    {
        public static string tenantID = "b6f16b85-9472-48bd-85f5-0dd35eafbb50";
        public static string authenticationKey = "otGOzlQR+CTHpFAl6flh2uUSudUoygRvg80UAPNBE6Q=";
        public static string applicationId = "653a7872-277b-4f6b-bbff-9163c76259b2";
        public static string subscriptionId = "13c5fec6-29cf-4abf-9a64-f01376778ba9";
        public static string dataFactoryName = "datafactorybi-v2-00";
        public static string resourceGroup = "gr_bi_dw_00";
        public static string onPremiseIntegrationRuntime = "Gateway01ADFv2-Test";
        public static string azureSSISIntegrationRuntime = "AzureSSIS-00";
        public static string azureIntegrationRuntime = "GatewayEnAzure";
        public static string linkedServiceSQLServer = "SqlServerLinkedService-Claim";
        public static string linkedServiceLake = "DataLakeStore-LinkedService";
        public static string linkedServiceSSIS = "AzureSSIS-LinkedService";
        public static string BDReferencia = "Data Source = DESKTOP-M220HEV\\SQLEXPRESS; Initial Catalog = ClaimCenter; User ID = supersa; Password=martin123";
        public static string[] tablasEspeciales = {
            "cc_checkpayee",
            "cc_vehicleowner",
            "cc_history",
            "cc_instrumentedworkertask",
            "cc_message",
            "cc_messagehistory",
            "cc_servicerequestdocumentlink",
            "cc_periodpolicy",
            "cc_zone",
            "cc_servicereqstatementdoclink",
            "cc_zone_link",
            "cc_transsetdocument",
            "cc_upgradetableregistry",
            "cc_processhistory",
            "cc_activitydocument",
            "cc_checksetreserve",
            "cc_paymentreserve",
            "cc_matterexposure",
            "cc_taccttxnhistory",
            "cc_reservelinewrapper",
            "cc_servreqactinst",
            "cc_claimaccess",
            "cc_claimexception"
            };

        public static string[] traerTablas(Boolean conSchema)
        {
            //Traigo cuantas tablas son
            SqlConnection con = new SqlConnection(DatosGrales.BDReferencia);
            String query = "select COUNT(*) from INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'";
            SqlCommand cmd = new SqlCommand(query, con);
            con.Open();
            int cant = (int)cmd.ExecuteScalar();
            con.Close();

            string[] ret = new string[cant];
            SqlConnection con1 = new SqlConnection(DatosGrales.BDReferencia);
            query = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE' order by TABLE_NAME asc";

            cmd = new SqlCommand(query, con);
            con.Open();
            SqlDataReader varReader = cmd.ExecuteReader();
            int o = 0;
            while (varReader.Read())
            {
                if (conSchema)
                {
                    ret[o] = "" + (string)varReader.GetValue(0) + "-" + (string)varReader.GetValue(1);
                }
                else
                {
                    ret[o] = (string)varReader.GetValue(1);
                }
                o++;
            }
            con.Close();

            return ret;

        }

        public static string queryMagica(string tabla, int filas)
        {
            string cantFilas;
            if (filas > 0)
            {
                cantFilas = " TOP " + filas + " ";
            }
            else
            {
                cantFilas = "";
            }

            string consulta = @"declare @tabla varchar(100)='" + tabla + @"'
                declare @query1 nvarchar(MAX)
                declare @pos bigint
                declare @texto varchar(1000)
                declare c1 cursor for
                SELECT -1,' SELECT " + cantFilas + @"' as Texto
                UNION
                SELECT C.COLUMN_ID
	            	,CASE WHEN C.column_id=1 THEN '' ELSE ',' END 
	                + CASE 	WHEN C.user_type_id IN (167,231) THEN 'REPLACE(REPLACE(REPLACE('+C.name+', CHAR(13),'' ''), CHAR(10), '' ''), ''|'', '''')' 
			        WHEN C.user_type_id in (130) THEN 'CAST('+C.name+' as nvarchar(4000)) ' 
			        else c.name end 
	                + ' as [' + C.name+ ']'
		        FROM SYS.COLUMNS C
                WHERE C.OBJECT_ID= OBJECT_ID(@tabla)
                UNION
                SELECT 10000,' FROM '+@tabla
                ORDER BY 1
                open c1
                FETCH NEXT FROM c1
                INTO @pos, @texto
                set  @query1=''

                WHILE @@FETCH_STATUS = 0  
                BEGIN  
	                set @query1=@query1+@texto
	                FETCH NEXT FROM c1
	                INTO @pos, @texto
                end
                close c1
                deallocate c1
                exec sp_executesql @stmt=@query1";
            return consulta;
        }
    }
}
