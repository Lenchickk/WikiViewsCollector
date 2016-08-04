using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;

namespace vpUltimate
{
    public class Downloader
    {
        private volatile bool _shouldStop;
        private List<string> pages;
        public static int countofUs = -1;
        public int mynumber;
        public void RequestStop()
        {
            _shouldStop = true;
        }
        // Volatile is used as hint to the compiler that this data
        // member will be accessed by multiple threads.

        public Downloader(List<string> p)
        {
            pages = new List<string>(p);
            countofUs++;
            mynumber = countofUs;
        }

        public void DownloadStreamUpdate()
        {
            Console.WriteLine("Downloader started! My number is " + mynumber.ToString());

            // for (int i = 0; i < pages.Count; i++)
            foreach (String pagee in pages)
            {


                WebClient w = new WebClient();
                while (BunchHelper.outFile > 4)
                {
                    ;
                }
                while (BunchHelper.rawFile > 4)
                {
                    ;
                }
                String s = w.DownloadString(pagee);
                Int16 day = 0;
                Boolean flag = true;
                Int16 counter = -1;
                char[] delimiterChars = { '-', ' ' };
                Int32 index = -1;

                foreach (LinkItem li in LinkFinder.Find(s))
                {
                    if (li.Href[0] != 'p') continue;

                    if (day == 1 && flag)
                    {
                        flag = false;
                        continue;
                    };
                    counter++;
                    String[] check = li.Text.Split(delimiterChars);
                    Int16 validationHour = Int16.Parse(check[2].Substring(0, 2));
                    if (counter != validationHour) counter = validationHour;
                    if (counter == 24) counter = 0;
                    if (counter != 19) continue;
                    String address = pagee + li.Href;
                    index++;
                    String to = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug\" + li.Href;// "_" + index.ToString();

                    day++;


                    if (BunchHelper.startedDownload.ContainsKey(li.Href))
                    {
                        continue;
                    }

                    BunchHelper.startedDownload.Add(li.Href, true);


                    using (var client = new WebClient())
                    {
                        BunchHelper.rawFile++;
                        // while (BunchHelper.rawFile >= 3)
                        // { ;}
                        Console.WriteLine(" I am downloading " + li.Href);
                    trymore:
                        try { client.DownloadFile(address, to); }
                        catch (System.Net.WebException ex) { System.Threading.Thread.Sleep(10000); goto trymore; }

                    }

                    BunchHelper.downloaded.Add(li.Href, true);
                    BunchHelper.starter = true;
                }



            }
            Console.WriteLine("Downloader ended");
            BunchHelper.downloadThreadEnd = true;


        }



        public void DownloadStream()
        {
            Console.WriteLine("Downloader started! My number is " + mynumber.ToString());

           // for (int i = 0; i < pages.Count; i++)
            foreach (String pagee in pages)
            {


                WebClient w = new WebClient();
                while (BunchHelper.outFile > 4)
                {
                    ;
                }
                while (BunchHelper.rawFile > 4)
                {
                    ;
                }
                String s = w.DownloadString(pagee);
                Int16 day = 0;
                Boolean flag = true;
                Int16 counter = -1;
                char[] delimiterChars = { '-', ' ' };
                Int32 index = -1;

                foreach (LinkItem li in LinkFinder.Find(s))
                {
                    if (li.Href[0] != 'p') continue;
                   
                    if (day == 1 && flag)
                    {
                        flag = false;
                        continue;
                    };
                    counter++;
                    String[] check = li.Text.Split(delimiterChars);
                    Int16 validationHour = Int16.Parse(check[2].Substring(0, 2));
                    if (counter != validationHour) counter = validationHour;
                    if (counter == 24) counter = 0;
                    if (counter != 19) continue;
                    String address = pagee + li.Href;
                    index++;
                    String to = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug\" + li.Href;// "_" + index.ToString();

                    day++;


                    if (BunchHelper.startedDownload.ContainsKey(li.Href))
                    {
                        continue;
                    }

                    BunchHelper.startedDownload.Add(li.Href, true);


                    using (var client = new WebClient())
                    {
                        BunchHelper.rawFile++;
                        // while (BunchHelper.rawFile >= 3)
                        // { ;}
                        Console.WriteLine("I am downloading " + li.Href);
                    trymore: 
                        try { client.DownloadFile(address, to); }
                        catch (System.Net.WebException ex) { System.Threading.Thread.Sleep(10000); goto trymore; }
                      
                    }

                    BunchHelper.downloaded.Add(li.Href, true);
                    BunchHelper.starter = true;
                }



            }
            Console.WriteLine("Downloader ended");
            BunchHelper.downloadThreadEnd = true;


        }


        public void DownloadStream2()
        {
            //Console.WriteLine("Downloader started! My number is " + mynumber.ToString());
            //if (mynumber.ToString() == "2")
            //{

            //}
            WebClient w = new WebClient();
            foreach (String pagee in pages)
            {


               
                while (BunchHelper.outFile > 3)
                {
                    ;
                }
                while (BunchHelper.rawFile > 8)
                {
                    ;
                }
                String s = w.DownloadString(pagee);
                Int16 day = 0;
                Boolean flag = true;
                Int16 counter = -1;
                char[] delimiterChars = { '-', ' ' };
                Int32 index = -1;

                foreach (LinkItem li in LinkFinder.Find(s))
                {
                    if (li.Href[0] != 'p') continue;

                    if (day == 1 && flag)
                    {
                        flag = false;
                        continue;
                    };
                    counter++;
                    String[] check = li.Text.Split(delimiterChars);


                    while (BunchHelper.rawFile > 8)
                    {
                        ;
                    }
                    Int16 validationHour = Int16.Parse(check[2].Substring(0, 2));
                    if (counter != validationHour) counter = validationHour;
                    if (counter == 24) counter = 0;
                    //if (counter != 19) continue;
                    String address = pagee + li.Href;
                    index++;
                    String to = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug\" + li.Href;// "_" + index.ToString();

                    day++;


                    if (BunchHelper.startedDownload.ContainsKey(li.Href))
                    {
                        continue;
                    }

                    BunchHelper.startedDownload.Add(li.Href, true);


                    //using (var client = new WebClient())
                    //{
                        BunchHelper.rawFile++;
                        // while (BunchHelper.rawFile >= 3)
                        // { ;}
                        Console.WriteLine("I am " + mynumber.ToString() + " and I am downloading " + li.Href);
                    trymore:
                        try { w.DownloadFile(address, to); }
                        catch (System.Net.WebException ex) { System.Threading.Thread.Sleep(10000); goto trymore; }

                    //}

                    BunchHelper.downloaded.Add(li.Href, true);
                    BunchHelper.starter = true;
                }



            }
            Console.WriteLine("I am " + mynumber.ToString() + "and I am done.");
            BunchHelper.downloadThreadEnd = true;


        }


        public string GetTask()
        {
            if (BunchHelper.tasks.Count == 0) return "done";
            String myTask = BunchHelper.tasks[0];
            BunchHelper.tasks.RemoveAt(0);
            BunchHelper.completasks.Add(myTask);
            return (myTask);
           
        }


        public void DownloadStream2Update()
        {
            //Console.WriteLine("Downloader started! My number is " + mynumber.ToString());
            //if (mynumber.ToString() == "2")
            //{

            //}
            WebClient w = new WebClient();
            Int16 day = 0;
            Boolean flag = true;
            Int16 counter = -1;
            char[] delimiterChars = { '-', ' ' };
            Int32 index = -1;
            String task = "";
            //foreach (String task in BunchHelper.tasks)
            while ((task = GetTask())!="done")
            {

                while (BunchHelper.outFile > 3)
                {
                    ;
                }
                while (BunchHelper.rawFile > 8)
                {
                    ;
                }



                counter++;
                String[] buff = task.Split('/');
                String myname = buff[buff.Length - 1];
                String[] check = myname.Split(delimiterChars);

             
                Int16 validationHour = Int16.Parse(check[2].Substring(0, 2));
                if (counter != validationHour) counter = validationHour;
                if (counter == 24) counter = 0;
                //if (counter != 19) continue;
                



                index++;
                String to = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug\" + myname;

                if (BunchHelper.startedDownload.ContainsKey(myname))
                {
                    continue;
                }

                BunchHelper.startedDownload.Add(myname, true);
                BunchHelper.rawFile++;

                Console.WriteLine("I am " + mynumber.ToString() + " and I am downloading " + myname);
            trymore:
                try { w.DownloadFile(task, to); }
                catch (System.Net.WebException ex) { System.Threading.Thread.Sleep(10000); goto trymore; }

                //}

                BunchHelper.downloaded.Add(myname, true);
                BunchHelper.starter = true;

            }

                  



            
            Console.WriteLine("I am " + mynumber.ToString() + "and I am done.");
            BunchHelper.downloadThreadEnd = true;


        }
    }
}
