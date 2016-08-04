using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;


namespace vpUltimate
{
    static public class Exporter
    {
        static public SortedDictionary<DateTime, StaticInfo> storageTable=new SortedDictionary<DateTime,StaticInfo>();
        static public SortedDictionary<DateTime, List<StaticInfo>> storageTabletreats = new SortedDictionary<DateTime, List<StaticInfo>>();

        static public void Export()
        {
            StreamWriter sw;
            DataView dv = BunchHelper.resulttable.DefaultView;
            dv.Sort = "groupName asc, name asc, year asc, month asc, day asc";
            BunchHelper.resulttable = dv.ToTable();
            //String ou = @"d:\Users\Lenchick\Google Drive\WASHU FALL2015\play ground\pipe\__trial3_pageviews\";
            String ou = @"d:\wp\pipe\pageviews\";

            foreach (DataRow r in BunchHelper.resulttable.Rows)
            {
                //if (!File.Exists(ou + r["output"].ToString() + "_" +  r["month"].ToString() + "_" +  r["year"].ToString() + ".csv"))
                if (!File.Exists(ou + r["output"].ToString() + "_" + r["year"].ToString() + ".csv"))
                {
                    //sw = new StreamWriter(ou + r["output"].ToString() + "_" + r["month"].ToString() + "_" + r["year"].ToString() + ".csv", false);
                    sw = new StreamWriter(ou + r["output"].ToString() + "_" + r["year"].ToString()+".csv", false);
                    sw.WriteLine("index\tID\tgroup_ID\tgroup_name\toutput_file\tdomain\tname\tfullname\tyear\tmonth\tday\tviews\tsize\tncontributors\tnchanges\tnchangesm\tnreverted\tnprotected");
                }
                else
                {
                    //sw = new StreamWriter(ou + r["output"].ToString() + "_" + r["month"].ToString() + "_" + r["year"].ToString() + ".csv", true);
                    sw = new StreamWriter(ou + r["output"].ToString() + "_" + r["year"].ToString() + ".csv", true);
                }
                sw.WriteLine(RowToCSV(r, BunchHelper.resulttable.Columns.Count));
                sw.Close();
            }


            BunchHelper.CreateResultTable();
        }

        static public void ExportAverages(String domain, DateTime date, Double pageViews, Double pageSize, Double pageViewsNN, Int64 n, Int64 nNN)
        {
            StreamWriter sw;
            String ou = @"d:\Users\Lenchick\Google Drive\WASHU FALL2015\play ground\pipe\_domainAverages\";
            Boolean b = File.Exists(ou + domain + "_dailyMeans.csv");
            sw = new StreamWriter(ou+domain + "_dailyMeans.csv", true);
            if (!b) sw.WriteLine("domain,year,month,day,n,pageViews,pageSize");
            sw.WriteLine(domain + ","  + date.Year + "," + date.Month + "," + date.Day + "," +  n.ToString() + "," + pageViews.ToString() + "," + pageSize.ToString());
            sw.Close();
        }

        static public void ExportTotals(String domain, DateTime date, Int64 pageViews, Int64 pageSize, Int64 n)
        {
            StreamWriter sw;
            String ou = @"D:\wp\pipe\domainTotal\";
            Boolean b = File.Exists(ou + domain + "_dailyMeans.csv");
            sw = new StreamWriter(ou + domain + "_dailyTotal.csv", true);
            if (!b) sw.WriteLine("domain,year,month,day,n,pageViews,pageSize");
            sw.WriteLine(domain + "," + date.Year + "," + date.Month + "," + date.Day + "," + n.ToString() + "," + pageViews.ToString() + "," + pageSize.ToString());
            sw.Close();
        }

        static public void ExportStatisticsTable(String domain)
        {
            
            String ou = @"D:\wp\pipe\domainTotal\";
            StreamWriter sw = new StreamWriter(ou+"_"+domain+".csv",false);
            sw.WriteLine("year,month,day,n,pageViews,pageSize");
           
            foreach (DateTime dt in storageTable.Keys)
            {
                sw.WriteLine(dt.Year + "," + dt.Month + "," + dt.Day + "," + storageTable[dt].n.ToString() + "," + storageTable[dt].views.ToString() + "," + storageTable[dt].megabytes.ToString());
            }

            sw.Close();
        }



        static public void ExportStatisticsTableExtended(String domain)
        {

            String ou = @"D:\wp\pipe\domainTotal\";
            StreamWriter sw = new StreamWriter(ou + "_" + domain + ".csv", false);
            String line = "year,month,day,n,pageViews,pageSize";
            for (int i = 1; i < 13; i++)
            {
                String str = i.ToString();
                line += "," + str + "n," + str + "pageviews," + str + "pagesize";
            }

            sw.WriteLine(line);

            foreach (DateTime dt in storageTable.Keys)
            {
                line = dt.Year + "," + dt.Month + "," + dt.Day + "," + storageTable[dt].n.ToString() + "," + storageTable[dt].views.ToString() + "," + storageTable[dt].megabytes.ToString();

                foreach (StaticInfo si in storageTabletreats[dt])
                {
                    line += "," + si.n + "," + si.views + "," + si.megabytes;
                }

                sw.WriteLine(line);
            }

            sw.Close();
        }

        static public String RowToCSV(DataRow r, int n)
        {
            String str = "";
            for (int i = 0; i < n; i++)
            {
                str += r[i].ToString() + "\t";
            }
            return (str.Substring(0, str.Length - 1));
        }
    }


}
