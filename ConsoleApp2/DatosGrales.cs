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
        public static string tenantID;
        public static string authenticationKey;
        public static string applicationId;
        public static string subscriptionId;
        public static string dataFactoryName;
        public static string resourceGroup;
        public static string onPremiseIntegrationRuntime;
        public static string azureSSISIntegrationRuntime;
        public static string azureIntegrationRuntime;
        public static string linkedServiceSQLServer;
        public static string linkedServiceLake;
        public static string linkedServiceSSIS;
        public static string BDReferencia;
        public static string nombreBD;
        public static string[] tablasEspeciales = { };
        public static string[] tablasLakeaWarehouse = { };
        public static string linkedServiceWarehouse = "SqlServerLinkedServiceWarehouse";
        public static string warehouseConnectionString;
        public static string usuarioOnPremise;
        public static string passwordOnPremise;
        public static string nombreBDWarehouse;
        public static string usuarioWarehouse;
        public static string passwordWarehouse;


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
	                + CASE 	WHEN C.user_type_id IN (167,231) THEN 'CAST(REPLACE(REPLACE(REPLACE(['+C.name+'], CHAR(13),'' ''), CHAR(10), '' ''), ''|'', '''') as varchar(2500))' 
			        WHEN C.user_type_id in (130) THEN 'CAST(['+C.name+'] as varchar(1000)) ' 
			        else '['+c.name+']' end 
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

        public static string traerCamposPolybase(string nombreTabla)
        {
            string ret = "";
            SqlConnection con1 = new SqlConnection(DatosGrales.BDReferencia);
            string query = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='" + nombreTabla + "' order by ORDINAL_POSITION";
            string campoActual;
            var cmd = new SqlCommand(query, con1);
            con1.Open();
            SqlDataReader varReader = cmd.ExecuteReader();
            
            while (varReader.Read())
            {
                campoActual = (string)varReader.GetValue(0) + ": " + (string)varReader.GetValue(0) +", ";
                ret = ret + campoActual;
            }

            ret = ret.Substring(0, ret.Length - 1);
            con1.Close();

            return ret;
        }

       
    }
}
