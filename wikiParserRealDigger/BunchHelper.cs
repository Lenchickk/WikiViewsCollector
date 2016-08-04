using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Threading;
using System.IO;
using System.Net;

namespace vpUltimate
{
    class BunchHelper
    {
        ArticleDate start;
        ArticleDate end;
        String str;
        DateTime cursor;
        static public List<string> pages;
        static String bas = "http://dumps.wikimedia.org/other/pagecounts-raw/";
        List<string> years;
        List<string> months;
        static public Dictionary<Int64,DateTime> borns;
        static public volatile DataTable datatable;
        static public volatile DataTable resulttable;
        static public volatile Dictionary<String, Boolean> downloaded;
        static public volatile Dictionary<String, Boolean> unwrapped;
        static public volatile bool downloadThreadEnd = false;
        static public volatile bool unwrapperThreadEnd = false;
        static public volatile bool starter = false;
        static public volatile bool wrapstart = false;
        static public volatile Dictionary<String, Boolean> startedDownload;
        static public volatile Dictionary<String, Boolean> startedUnwrap;
        static public volatile List<Boolean> synch;
        static public volatile List<Thread> downloaderThreads;
        static public volatile Dictionary<Int64, Int64> pagesizes;
        static public volatile int rawFile = 0;
        static public volatile int outFile = 0;
        static public string Start;
        static public string End;
        static public DateTime start_;
        static public DateTime end_;
        static public String Base = "";
        static public volatile List<String> tasks;
        static public volatile List<String> completasks;

        public void processPipeFile(String[] file, String dd="")
        {

            try
            {
                RulesDictionary.CreateSelectedList(@"D:\wp\pipe\treatmentinput\treatmentgroups.txt");
            }
            catch (Exception ex)
            {

            }

            start_ = DateTime.ParseExact(Start, "MM/yyyy", null);
            end_ = DateTime.ParseExact(End, "MM/yyyy", null);
            
            start = new ArticleDate(DateTime.ParseExact(Start, "MM/yyyy", null));
            end = new ArticleDate(DateTime.ParseExact(End, "MM/yyyy", null));
            borns = new Dictionary<Int64,DateTime>();
            pagesizes = new Dictionary<long, long>();
            completasks = new List<string>();
            startedUnwrap = new Dictionary<string, bool>();
            if (start > end) return;
            //CreateRange(start, end);
            //CreateRange2();
            tasks = CreateRangeLinks(start, end); 
            //ListGoesRelative(file);
            ListsGoesRelative(file);
            
            StartThreads(5);


            while (!starter)
            { }

            for (int i = 0; i < 2; i++)
            {
                UnWrapper unwrapper = new UnWrapper();
                System.Threading.Thread unwrapStream = new System.Threading.Thread(unwrapper.UnWrapperStream);
                unwrapStream.Start();
            }

           while (!wrapstart)
            { }
           // 

            CreateResultTable();
            

            Parser parser = new Parser();
            //parser.DoParsing2();
            //parser.DoParsing_meanDomain2("ru");
            parser.DoParsing_meanDomain2SpeedUpTakeFull("ru", "fast2015_5_16_4");
        }

        static public void CreateResultTable()
        {
            resulttable = new DataTable();
            resulttable.Columns.Add("index", typeof(Int64));
            resulttable.Columns.Add("ID", typeof(Int64));
            resulttable.Columns.Add("groupID", typeof(int));
            resulttable.Columns.Add("groupName", typeof(string));
            resulttable.Columns.Add("output", typeof(string));
            resulttable.Columns.Add("domain", typeof(string));
            resulttable.Columns.Add("name", typeof(string));
            resulttable.Columns.Add("fullname", typeof(string));
            resulttable.Columns.Add("year", typeof(Int16));
            resulttable.Columns.Add("month", typeof(Int16));
            resulttable.Columns.Add("day", typeof(Int16));
            resulttable.Columns.Add("views", typeof(Int64));
            resulttable.Columns.Add("size", typeof(Int64));
            resulttable.Columns.Add("nchanges", typeof(Int64));
            resulttable.Columns.Add("ncontributers", typeof(Int16));
            resulttable.Columns.Add("contributers", typeof(string));
            resulttable.Columns.Add("changes", typeof(string));
            resulttable.Columns.Add("talk", typeof(string));
        }


        void ListGoesRelative(String s)
        {
            createMainTable();
            char[] delimiterChars = { '\t' };

            StreamReader sr = new StreamReader(s);
            String str;
            sr.ReadLine();
            Int64 index = 0;

            while (((str = sr.ReadLine()) != null) && (str != ""))
            {
                index++;
                String[] input = str.Split(delimiterChars);
                Int64 ID = Int64.Parse(input[0]);
                Int64 groupID = Int64.Parse(input[1]);
                String groupName = input[2];
                String output = groupName ;
                String domain = input[3];
                String name = input[4];
                String fullname = input[5];
                String searchstring = domain + " " + name;
                String headstring = str;
                String year = input[11];
                String day = input[12];
                String month = input[13];
                datatable.Rows.Add(index, ID, groupID, groupName, output, domain, name, fullname,searchstring, headstring, year, day,month);
                borns.Add(index, new DateTime(Int32.Parse(year),Int32.Parse(month),Int32.Parse(day)));
                pagesizes.Add(index, 0);
           }

            DataView dv = datatable.DefaultView;
            dv.Sort = "searchstring asc";
            datatable = dv.ToTable();
        }


        void ListsGoesRelative(String[] ss)
        {
            createMainTable();
            char[] delimiterChars = { '\t' };
            Int64 index = 0;
        

            foreach (String s in ss)
            {
                StreamReader sr = new StreamReader(s);
                String str;
                sr.ReadLine();
         

                while (((str = sr.ReadLine()) != null) && (str != ""))
                {
                    index++;
                    String[] input = str.Split(delimiterChars);
                    Int64 ID = Int64.Parse(input[0]);
                    Int64 groupID = Int64.Parse(input[1]);
                    String groupName = input[2];
                    String domain = input[3];
                    String name = input[4];
                    String output = groupName + "_" + domain + "_" + input[0];
                    String fullname = input[5];
                    String searchstring = domain + " " + name ;
                    String headstring = str;
                    String year = input[11];
                    String day = input[12];
                    String month = input[13];
                    datatable.Rows.Add(index, ID, groupID, groupName, output, domain, name, fullname, searchstring, headstring, year, day, month);
                    borns.Add(index, new DateTime(Int32.Parse(year), Int32.Parse(month), Int32.Parse(day)));
                    pagesizes.Add(index, 0);
                }
            }

            DataView dv = datatable.DefaultView;
            dv.Sort = "searchstring asc";
            datatable = dv.ToTable();
 
        }



        void CreateRange(ArticleDate start, ArticleDate end)
        {

            pages = new List<string>();
            years = new List<string>();
            months = new List<string>();
            DateTime cursor = new DateTime(start.time.Ticks);
            downloaded = new Dictionary<string, bool>();
            unwrapped = new Dictionary<string, bool>();
            startedDownload = new Dictionary<string, bool>();
            synch = new List<bool>();
            synch.Add(false);
            synch.Add(false);


            while (cursor.Ticks <= end.time.Ticks)
            {
                String line = bas;
                years.Add(cursor.Year.ToString());
                months.Add(cursor.Month.ToString());
                //if (cursor.Year < 2010) line += "0";
                line += cursor.Year.ToString() + "/" + cursor.Year.ToString() + "-";
                if (cursor.Month < 10) line += "0";
                line += cursor.Month.ToString() + "/";
                pages.Add(line);
                cursor = PlusMonth(cursor);
            }


        }


        List<String> CreateRangeLinks(ArticleDate start, ArticleDate end)
        {

            pages = new List<string>();
            years = new List<string>();
            months = new List<string>();
            DateTime cursor = new DateTime(start.time.Ticks);
            downloaded = new Dictionary<string, bool>();
            unwrapped = new Dictionary<string, bool>();
            startedDownload = new Dictionary<string, bool>();
            synch = new List<bool>();
            synch.Add(false);
            synch.Add(false);
            List<String> links = new List<String>();

            while (cursor.Ticks <= end.time.Ticks)
            {
                String line = bas;
                years.Add(cursor.Year.ToString());
                months.Add(cursor.Month.ToString());
                //if (cursor.Year < 2010) line += "0";
                line += cursor.Year.ToString() + "/" + cursor.Year.ToString() + "-";
                if (cursor.Month < 10) line += "0";
                line += cursor.Month.ToString() + "/";
                pages.Add(line);
                cursor = PlusMonth(cursor);
            }

            /*update*/
            WebClient w = new WebClient();
            foreach (String pagee in pages)
            {
                String s = w.DownloadString(pagee);
                foreach (LinkItem li in LinkFinder.Find(s))
                {
                    if (li.Href[0] != 'p') continue;
                    links.Add(pagee+li.Href);


                }
            }

            return (links);
        }

        void CreateRange2()
        {

            pages = new List<string>();
            years = new List<string>();
            months = new List<string>();
            downloaded = new Dictionary<string, bool>();
            unwrapped = new Dictionary<string, bool>();
            startedDownload = new Dictionary<string, bool>();
            synch = new List<bool>();
            synch.Add(false);
            synch.Add(false);


           // months.Add("04");
           // months.Add("06");
           // months.Add("11");
            months.Add("09");

            //years.Add("2013");
            //years.Add("2013");
            //years.Add("2013");
            years.Add("2015");

            //pages.Add(@"http://dumps.wikimedia.org/other/pagecounts-raw/2013/2013-04/");
            //pages.Add(@"http://dumps.wikimedia.org/other/pagecounts-raw/2013/2013-06/");
            //pages.Add(@"http://dumps.wikimedia.org/other/pagecounts-raw/2013/2013-11/");
            pages.Add(@"http://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-09/");


        }



        void StartThreads(int k)
        {

            int step = pages.Count / k;

            if (step * k < pages.Count) k = k + 1;
            List<List<string>> buf = new List<List<string>>(k);
            bool first = true;
            int counter = -1;
            int num = 0;
            buf.Add(new List<string>());
            if (step == 0) step = 1000;

            for (int i = 0; i < pages.Count; i++)
            {
                counter++;

                if ((counter > step - 1))
                {
                    num++;
                    counter = -1;
                    buf.Add(new List<string>());
                }
                buf[num].Add(pages[i]);
            }
            downloaderThreads = new List<Thread>();

            for (int m = 0; m < num + 1; m++)
            {
                Downloader downloader = new Downloader(buf[m]);
                System.Threading.Thread downloadStream = new System.Threading.Thread(downloader.DownloadStream2Update);
                downloadStream.Start();
                downloaderThreads.Add(downloadStream);
                //System.Threading.Thread.Sleep(3000);

            }
        }



        void StartThreadsOld(int k)
        {

            int step = pages.Count / k;
           
            if (step * k < pages.Count) k = k + 1;
            List<List<string>> buf = new List<List<string>>(k);
            bool first = true;
            int counter = -1;
            int num = 0;
            buf.Add(new List<string>());
            if (step == 0)
            {
                step =1000;
                
            }
            for (int i = 0; i < pages.Count; i++)
            {
                counter++;

                if ((counter > step-1) && first)
                {
                    num++;
                    counter = -1;
                    first = false;
                    buf.Add(new List<string>());
                }
                buf[num].Add(pages[i]);
            }
            downloaderThreads = new List<Thread>();
           
            for (int m = 0; m < num+1; m++)
            {
                Downloader downloader = new Downloader(buf[m]);
                System.Threading.Thread downloadStream = new System.Threading.Thread(downloader.DownloadStream2);
                downloadStream.Start();
                downloaderThreads.Add(downloadStream);
                System.Threading.Thread.Sleep(1000);

            }
        }


        void StartThreads2(int k)
        {

            int step = pages.Count / k;
            if (step * k < pages.Count) k = k + 1;
            List<List<string>> buf = new List<List<string>>(k);
            int num = 0;
            for (int i = 0; i < k; i++ ) buf.Add(new List<string>());
            for (int i = 0; i < pages.Count; i++)
            {
                buf[i/step].Add(pages[i]);
            }
            downloaderThreads = new List<Thread>();

            for (int m = 0; m < k; m++)
            {
                Downloader downloader = new Downloader(buf[m]);
                System.Threading.Thread downloadStream = new System.Threading.Thread(downloader.DownloadStream);
                downloadStream.Start();
                downloaderThreads.Add(downloadStream);
                //System.Threading.Thread.Sleep(1000);

            }
        }

        
        DateTime PlusMonth(DateTime dt)
        {
            Int32 month = dt.Month;
            if (month == 12)
            {
                return (new DateTime(dt.Year + 1, 1, 1));
            }

            return (new DateTime(dt.Year, month + 1, 1));
        }

        static void createMainTable()
        {
            datatable = new DataTable();
            datatable.Columns.Add("index", typeof(Int64));
            datatable.Columns.Add("ID", typeof(Int64));
            datatable.Columns.Add("groupID", typeof(int));
            datatable.Columns.Add("groupName", typeof(string));
            datatable.Columns.Add("output", typeof(string));
            datatable.Columns.Add("domain", typeof(string));
            datatable.Columns.Add("name", typeof(string));
            datatable.Columns.Add("fullname", typeof(string));
            datatable.Columns.Add("searchstring", typeof(string));
            datatable.Columns.Add("headstring", typeof(string));
            datatable.Columns.Add("year", typeof(string));
            datatable.Columns.Add("day", typeof(string));
            datatable.Columns.Add("month", typeof(string));
        }
    }
}
