using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;

namespace vpUltimate
{
    class Parser
    {
        private volatile bool _shouldStop;
        public void RequestStop()
        {
            _shouldStop = true;
        }

        public void DoParsing()
        {
            String thruDirectory = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug\";
            
            String[] viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            DateTime currentDate;
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            String[] buf;
            Int32 cursor = 0;
            String searchstring;
            String previousDomain = "";
            Int32 buffersize = 40000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            Int64 count = 0;
            String shortname;
            String currentDomain = "";
            StreamWriter errors;
            String currentname;
            Int64 workCounter = 0;


            Console.WriteLine("Parsing started!");

            while (cursor != BunchHelper.datatable.Rows.Count)
            {

                foreach (String file in viewsFiles)
                {
                    buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];

                    buf = file.Split(delimiterLast);
                    currentDate = DateTime.ParseExact(buf[1], "yyyyMMdd", null);

                    StreamReader sr = new StreamReader(file);
                    String previos_tail = "";
                    String tail = "";
                    int pos = -1;
                    Int32 pageviews;
                    Int64 pagesize;
                    int a;

                    DataRow currentRow;
                    String str = "";
                back:
                    if (cursor == BunchHelper.datatable.Rows.Count) goto done;

                    currentRow = BunchHelper.datatable.Rows[cursor];

                    if (BunchHelper.borns[Int64.Parse(currentRow["index"].ToString())] > currentDate )
                    {
                        cursor++;
                        goto back;
                    }

                    searchstring = currentRow["searchstring"].ToString();
                    currentDomain = currentRow["domain"].ToString();
                    currentname = currentRow["name"].ToString();
                    
                    String page = "http://" + currentDomain + ".wikipedia.org/wiki/" + currentname;


                    while (cursor != BunchHelper.datatable.Rows.Count)
                    {

                        while ((a = sr.ReadBlock(buffer, 0, buffersize)) > searchstring.Length)
                        {
                            if (buffer[buffersize - 1] != '\n')
                            {
                                tail = "";
                                for (int i = buffersize - 1; true; i--)
                                {
                                    if (buffer[i] == '\n') break;
                                    tail = buffer[i].ToString() + tail;
                                }
                            }

                            str = new String(buffer);
                            str = str.Substring(0, str.Length - tail.Length);
                            str = previos_tail + str;
                            previos_tail = tail;

                            if ((pos = str.IndexOf(searchstring)) != -1)
                            {

                                if (!Int32.TryParse(((str.Substring(pos + searchstring.Length + 1)).Split())[0], out pageviews))
                                {
                                    pageviews = 0;
                                    pagesize = -1;
                                }
                                else
                                {
                                    pageviews = Int32.Parse(((str.Substring(pos + searchstring.Length + 1, 40)).Split())[0]);
                                    pagesize = Int32.Parse(((str.Substring(pos + searchstring.Length + 1, 40)).Split())[1]);
                                }
                                count++;


                                BunchHelper.resulttable.Rows.Add(count, currentRow["ID"], currentRow["groupID"], currentRow["groupName"],
                                                                  currentRow["output"],
                                                                 currentRow["domain"], currentRow["name"], currentDate.Year.ToString(),
                                                                 currentDate.Month.ToString(), currentDate.Day.ToString(),
                                                                 pageviews, pagesize, -1, -1, "", "","");

                                Console.WriteLine(currentRow["domain"] + " " + currentRow["name"] + " " + pageviews.ToString() + " " + pagesize.ToString() + " " + workCounter.ToString());
                                workCounter++;
                                previousDomain = currentDomain;

                                if (workCounter == 30)
                                {
                                    //Exporter.Export();
                                    workCounter = 0;
                                }

                            back1:
                                cursor++;

                                if (cursor == BunchHelper.datatable.Rows.Count)
                                {
                                    goto done;
                                }

                                currentRow = BunchHelper.datatable.Rows[cursor];

                                if (BunchHelper.borns[Int64.Parse(currentRow["index"].ToString())] > currentDate)
                                {
                                    goto back1;
                                }

                   
                                searchstring = currentRow["searchstring"].ToString();
                                currentDomain = currentRow["domain"].ToString();
                                currentname = currentRow["name"].ToString();
                            
                            }

                      }
                        if (a == 0) goto done;

                    }
                done: ;
                    cursor = 0;
                    sr.Close();
                    System.IO.File.Delete(file);
                    BunchHelper.outFile--;
                    //viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");

                }
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
                if (viewsFiles.Length == 0) break;
            }
        }


        DateTime GetCurrentDate(String file)
        {
            char[] delimiterLast = { '-' };
            String[] buf = file.Split(delimiterLast);
            return(DateTime.ParseExact(buf[1], "yyyyMMdd", null));
        }

        DateTime GetCurrentDate2(String file)
        {
            char[] delimiterLast = { '-' };
            String[] buf = file.Split(delimiterLast);
            return (DateTime.ParseExact(buf[1], "yyyyMMdd", null));
        }


        Int32 UpdateCursor(Int32 c, DateTime dt, out DataRow row,out String searchstring)
        {
            searchstring = "";
            row = null;
            while (c < BunchHelper.datatable.Rows.Count-1)
            {
                c++;
                row = BunchHelper.datatable.Rows[c];
                if (dt>=BunchHelper.borns[Int64.Parse(row["index"].ToString())])
                {
                    searchstring = row["searchstring"].ToString();
                    break;
                }
                Console.WriteLine(row["fullname"].ToString() + " " + BunchHelper.borns[Int64.Parse(row["index"].ToString())].ToString() + " too early!" + c.ToString());

            } ;
            if (row == null) return BunchHelper.datatable.Rows.Count;
            return c;
        }

        void GetViewStat(String str, String searchstring, int pos, out Int64 pagesize, out Int32 pageviews)
        {
            //int len = str.Length - pos - searchstring.Length-1; 
            if (!Int32.TryParse(((str.Substring(pos + searchstring.Length + 1)).Split())[0], out pageviews))
            {
                pageviews = 0;
                pagesize = -1;
            }
            else
            {
                pageviews = Int32.Parse(((str.Substring(pos + searchstring.Length + 1)).Split())[0]);

                try { pagesize = Int64.Parse(((str.Substring(pos + searchstring.Length + 1)).Split())[1]); }
                catch (SystemException ex)
                {
                    pagesize = -1;
                }
            }
        }


        Boolean StringComparing(String s1, String s2)
        {
            
            char[] c1 = s1.ToCharArray();
            char[] c2 = s2.ToCharArray();
            bool b = true;
            int min;

            if (c1.Length > c2.Length)
            {
                min = c2.Length;
            }
            else
            {
                min = c1.Length;
            }

            for (int i = 0; i < min; i++)
            {
                b = b && (c1[1] > c2[1]);
            }
            return b;
        }


        public bool IsDomain(String str, String domain)
        {
            char[] delimeterspace = { ' ' };
            if (str.Split(delimeterspace)[0] == domain) return true;
            return false;
            
        }


        public Int64 GetTotal(Double InTotalSize, Int64 InTotalViews,
                    out Double totalSize, out Int64 totalViews, Int64 nIn, String entity)
        {
            double norm= Math.Pow(1024,-2);
            char[] delimeterspace = { ' ' };
            String[] bhere = entity.Split(delimeterspace);
            Int64 csize = 0;
            Int64 cview = 0;

            if (bhere.Length > 2)
            {
                Int64.TryParse(bhere[2], out cview);
            }
            if (bhere.Length > 3)
            {
                Int64.TryParse(bhere[3], out csize);
            }
            
            totalSize = InTotalSize + csize*cview*norm;
            totalViews = InTotalViews + cview;
  
            return (nIn+1);
        }


        public Int64 GetTotalUp(Double InTotalSize, Int64 InTotalViews,
            out Double totalSize, out Int64 totalViews, Int64 nIn, String entity)
        {
            double norm = Math.Pow(1024, -2);
            char[] delimeterspace = { ' ' };
            String[] bhere = entity.Split(delimeterspace);
            Int64 csize = 0;
            Int64 cview = 0;

            if (bhere.Length > 2)
            {
                Int64.TryParse(bhere[2], out cview);
            }
            if (bhere.Length > 3)
            {
                Int64.TryParse(bhere[3], out csize);
            }

            totalSize = InTotalSize + csize * cview * norm;
            totalViews = InTotalViews + cview;

            if (RulesDictionary.pageList.ContainsKey(bhere[1]))
            {
                for (int j = 0; j < RulesDictionary.pageList[bhere[1]].Count; j++)
                {
                    int iswhere= RulesDictionary.pageList[bhere[1]][j];
                    RulesDictionary.selectedList[j].n += 1 *iswhere;
                    RulesDictionary.selectedList[j].totalSize += csize * cview * norm*iswhere;
                    RulesDictionary.selectedList[j].totalView += cview * iswhere;
                }
            }

            return (nIn + 1);
        }



        public Int64 GetAverages(Double InAverageSize, Double InAverageViews, Double InAverageViewsNN, 
                out Double averageSize, out Double averageViews,out Double averageViewsNN, Int64 n, Int64 nNNin, out Int64 nNNout, String entity)
        {
            char[] delimeterspace = { ' ' };
            String[] bhere = entity.Split(delimeterspace);
            Int32 csize=0;
            Int32 cview=0;
            
            if (bhere.Length > 2)
            {
                Int32.TryParse(bhere[2], out cview);
            }
            if (bhere.Length > 3)
            {
                Int32.TryParse(bhere[3], out csize);
            }
            
            
            
            averageSize = (InAverageSize * (n - 1) + csize) / n;
            averageViews = (InAverageViews * (n - 1) + cview) / n;
            nNNout = nNNin;
            averageViewsNN = InAverageViewsNN;
            if (cview>0)
            {
               averageViewsNN = (InAverageViewsNN * (nNNout - 1) + cview) / nNNout;
               nNNout = nNNin + 1;
            }
            n++;
            return(n);
        }
        public void DoParsing_meanDomain(string domain)
        {
            String thruDirectory = @"D:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug";
            StreamWriter tracker;
            String[] viewsFiles;
            DateTime currentDate;
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            char[] delimiterstring = { '\n' };
            char[] delimeterspace = { ' ' };
            Int32 cursor = 0;
            Int32 buffersize = 40000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            Int64 count = 0;
            String shortname;
            String currentDomain = "";
            StreamWriter errors;
            String currentname;
            Int64 workCounter = 0;
            currentDate = BunchHelper.start_;
            String justincase = "";

            viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            Console.WriteLine("Parsing started!");

            //while (currentDate <= BunchHelper.end_) 
            while (BunchHelper.outFile > 0 || BunchHelper.rawFile > 0)
            {
                foreach (String file in viewsFiles)
                {
            
                    Double averageViews = 0;
                    Double averageSize = 0;
                    Double averageViewsNN = 0;
                    Int64 n = 1;
                    Int64 nNN = 1;
                    String[] buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];
                    if (!BunchHelper.unwrapped.ContainsKey(shortname)) continue;
                    buffersize = 40000;
                    StreamReader sr = new StreamReader(file, Encoding.ASCII);
                    currentDate = GetCurrentDate(file);
                    
                    Int32 pageviews = 0;
                    buffersize = 10000;
                    Int64 pagesize = 0;
                    String str = "";
                    int a;
                    Console.WriteLine(currentDate.ToString());
                    String searchstring = "\n" + domain + " ";
                    Int32 pos = 0;
                    String tail = "";
                    String previos_tail = "";

                    while ((a = sr.ReadBlock(buffer, 0, buffersize)) > 0)
                    {
                        if (buffer[buffersize - 1] != '\n')
                        {
                            tail = "";
                            for (int i = buffersize - 1; true; i--)
                            {
                                if (buffer[i] == '\n') break;
                                tail = buffer[i].ToString() + tail;
                            }
                        }

                        str = new String(buffer);
                        str = str.Substring(0, str.Length - tail.Length);
                        str = previos_tail + str;
                        previos_tail = tail;
                        //if ((pos = str.IndexOf(searchstring, StringComparison.InvariantCultureIgnoreCase)) != -1)
                            if ((pos = str.IndexOf(searchstring)) != -1)
                        {
                            //GetViewStat(str, searchstring, pos, out pagesize, out pageviews);
                            str = str.Substring(pos);
                            String[] entities = str.Split(delimiterstring);
                            Int32 csize=0;
                            Int32 cviews=0;

                            foreach (String entity in entities)
                            {
                                if (entity == "") continue;
                                String[] bhere = entity.Split(delimeterspace);
                                if (bhere.Length < 3) continue;
                                n = GetAverages(averageSize, averageViews,averageViewsNN, out averageSize, out averageViews, out averageViewsNN, n, nNN, out nNN, entity);
                            }
                            break;
                                
                        }

                    }
                    String thisLine = "";
                    thisLine = sr.ReadLine();
                    

                    while (((thisLine=sr.ReadLine())!="") && thisLine!=null && IsDomain(thisLine,domain))
                    {
                        n = GetAverages(averageSize, averageViews, averageViewsNN, out averageSize, out averageViews, out averageViewsNN, n, nNN, out nNN, thisLine);
                       //Console.WriteLine(n);
                    }

                    //buffersize = 40000;                    
                    sr.Close();
                    System.IO.File.Delete(file);
                    Exporter.ExportAverages(domain, currentDate, 24*averageViews, averageSize, averageViewsNN, n-1, nNN-1);
                    BunchHelper.outFile--;
                }
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");

            };

        }



        public void DoParsing_meanDomain2SpeedUp(string domain, string aname)
        {
            String thruDirectory = @"D:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug";
            String[] viewsFiles;
            DateTime currentDate, currentDay, storedDay;
            storedDay = new DateTime();
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            char[] delimiterstring = { '\n' };
            char[] delimeterspace = { ' ' };
            Int32 buffersize = 40000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            String shortname;
            currentDate = BunchHelper.start_;
            bool dayFlag = true;

            Int64 prn = 0;
            Int64 prtotalView = 0;
            Int64 prtotalSize = 0;

            viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            Console.WriteLine("Parsing started!");

            //while (currentDate <= BunchHelper.end_) 
            while (BunchHelper.outFile > 0 || BunchHelper.rawFile > 0)
            {
                foreach (String file in viewsFiles)
                {


                    Int64 n = 0;
                    Int64 totalView = 0;
                    Double totalSize = 0;
                    String[] buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];
                    if (!BunchHelper.unwrapped.ContainsKey(shortname)) continue;
                    buffersize = 40000;
                    StreamReader sr = new StreamReader(file, Encoding.ASCII);
                    currentDate = GetCurrentDate(file);
                    currentDay = new DateTime(currentDate.Year, currentDate.Month, currentDate.Day);
                    if (dayFlag)
                    {
                        storedDay = currentDay;
                        dayFlag = false;
                    }

                    
                    String str = "";
                    int a;
                    Console.WriteLine(currentDate.ToString());
                    String searchstring = "\n" + domain + " ";
                    Int32 pos = 0;
                    String tail = "";
                    String previos_tail = "";
                    String[] bhere;

                    while ((a = sr.ReadBlock(buffer, 0, buffersize)) > 0)
                    {
                        if (buffer[buffersize - 1] != '\n')
                        {
                            tail = "";
                            for (int i = buffersize - 1; true; i--)
                            {
                                if (buffer[i] == '\n') break;
                                tail = buffer[i].ToString() + tail;
                            }
                        }
                      
                        str = new String(buffer);
                        str = str.Substring(0, str.Length - tail.Length);
                        str = previos_tail + str;
                        previos_tail = tail;

                        if ((pos = str.IndexOf(searchstring)) != -1)
                        {
                            str = str.Substring(pos);
                            String[] entities = str.Split(delimiterstring);


                            foreach (String entity in entities)
                            {
                                if (entity == "") continue;
                                bhere = entity.Split(delimeterspace);
                                if (bhere.Length < 3) continue;
                                if (bhere[0] != domain) 
                                    goto done;
                                n = GetTotal(totalSize, totalView, out totalSize, out totalView, n, entity);
                            }
                           

                        }

                    }
        
                done:
                    //buffersize = 40000;    
                    if (Exporter.storageTable.ContainsKey(currentDay))
                    {
                        Exporter.storageTable[currentDay].Add(totalView, totalSize, n);

                    }
                    else
                    {
                        Exporter.storageTable.Add(currentDay, new StaticInfo(totalView, totalSize, n));
                    }
                    sr.Close();
                    System.IO.File.Delete(file);
                    BunchHelper.outFile--;
                    Exporter.ExportStatisticsTable(domain + "_" + aname);
                }
               
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");

            };

            Exporter.ExportStatisticsTable(domain + "_" + aname);
        }


        public void DoParsing_meanDomain2SpeedUpTakeFull(string domain, string aname)
        {
            String thruDirectory = @"D:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug";
            String[] viewsFiles;
            DateTime currentDate, currentDay, storedDay;
            storedDay = new DateTime();
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            char[] delimiterstring = { '\n' };
            char[] delimeterspace = { ' ' };
            Int32 buffersize = 40000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            String shortname;
            currentDate = BunchHelper.start_;
            bool dayFlag = true;

            Int64 prn = 0;
            Int64 prtotalView = 0;
            Int64 prtotalSize = 0;

            


            viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            Console.WriteLine("Parsing started!");

            //while (currentDate <= BunchHelper.end_) 
            while (BunchHelper.outFile > 0 || BunchHelper.rawFile > 0)
            {
                foreach (String file in viewsFiles)
                {


                    Int64 n = 0;
                    Int64 totalView = 0;
                    Double totalSize = 0;
                    RulesDictionary.NulifyList();
                    String[] buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];
                    if (!BunchHelper.unwrapped.ContainsKey(shortname)) continue;
                    buffersize = 40000;
                    StreamReader sr = new StreamReader(file, Encoding.ASCII);
                    currentDate = GetCurrentDate(file);
                    currentDay = new DateTime(currentDate.Year, currentDate.Month, currentDate.Day);
                    if (dayFlag)
                    {
                        storedDay = currentDay;
                        dayFlag = false;
                    }

                    if (currentDay.Month == 5 && currentDate.Day == 9)
                    {
                        int aa = 1;
                    }
                    String str = "";
                    int a;
                    Console.WriteLine(currentDate.ToString());
                    String searchstring = "\n" + domain + " ";
                    Int32 pos = 0;
                    String tail = "";
                    String previos_tail = "";
                    String[] bhere;

                    while ((a = sr.ReadBlock(buffer, 0, buffersize)) > 0)
                    {
                        if (buffer[buffersize - 1] != '\n')
                        {
                            tail = "";
                            for (int i = buffersize - 1; true; i--)
                            {
                                if (i < 0) break;
                                if (buffer[i] == '\n') break;
                                tail = buffer[i].ToString() + tail;
                            }
                        }

                        str = new String(buffer);
                        str = str.Substring(0, str.Length - tail.Length);
                        str = previos_tail + str;
                        previos_tail = tail;

                        if ((pos = str.IndexOf(searchstring)) != -1)
                        {
                            str = str.Substring(pos);
                            String[] entities = str.Split(delimiterstring);


                            foreach (String entity in entities)
                            {
                                if (entity == "") continue;
                                bhere = entity.Split(delimeterspace);
                                if (bhere.Length < 3) continue;
                                if (bhere[0] != domain)
                                    goto done;
                                n = GetTotalUp(totalSize, totalView, out totalSize, out totalView, n, entity);
                            }


                        }

                    }

                done:
                    //buffersize = 40000;    
                    if (Exporter.storageTable.ContainsKey(currentDay))
                    {
                        Exporter.storageTable[currentDay].Add(totalView, totalSize, n);
                        int i = 0;
                        foreach (ComplexPageViewTracker ct in RulesDictionary.selectedList)
                        {
                            Exporter.storageTabletreats[currentDay][i].Add(ct.totalView, ct.totalSize, ct.n);
                            i++;
                        }
                    }
                    else
                    {
                        Exporter.storageTable.Add(currentDay, new StaticInfo(totalView, totalSize, n));
                        Exporter.storageTabletreats.Add(currentDay, new List<StaticInfo>());

                        foreach (ComplexPageViewTracker ct in RulesDictionary.selectedList)
                        {
                            Exporter.storageTabletreats[currentDay].Add(new StaticInfo(ct.totalView,ct.totalSize,ct.n));
                        }
                        
                       
                        
                    }
                    sr.Close();
                    System.IO.File.Delete(file);
                    BunchHelper.outFile--;
                    Exporter.ExportStatisticsTableExtended(domain + "_" + aname);
                }

                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");

            };

            Exporter.ExportStatisticsTableExtended(domain + "_" + aname);
        }

        public void DoParsing_meanDomain2(string domain)
        {
            String thruDirectory = @"D:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug";
            String[] viewsFiles;
            DateTime currentDate, currentDay,storedDay;
            storedDay = new DateTime();
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            char[] delimiterstring = { '\n' };
            char[] delimeterspace = { ' ' };
            Int32 buffersize = 40000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            String shortname;
            currentDate = BunchHelper.start_;
            bool dayFlag = true;

            Int64 prn = 0;
            Int64 prtotalView = 0;
            Int64 prtotalSize = 0;

            viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            Console.WriteLine("Parsing started!");

            //while (currentDate <= BunchHelper.end_) 
            while (BunchHelper.outFile > 0 || BunchHelper.rawFile > 0)
            {
                foreach (String file in viewsFiles)
                {


                    Int64 n = 0;
                    Int64 totalView = 0;
                    Double totalSize = 0;
                    String[] buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];
                    if (!BunchHelper.unwrapped.ContainsKey(shortname)) continue;
                    buffersize = 40000;
                    StreamReader sr = new StreamReader(file, Encoding.ASCII);
                    currentDate = GetCurrentDate(file);
                    currentDay = new DateTime(currentDate.Year, currentDate.Month, currentDate.Day);
                    if (dayFlag)
                    {
                        storedDay = currentDay;
                        dayFlag = false;
                    }

                    buffersize = 10000;
                    String str = "";
                    int a;
                    Console.WriteLine(currentDate.ToString());
                    String searchstring = "\n" + domain + " ";
                    Int32 pos = 0;
                    String tail = "";
                    String previos_tail = "";

                    while ((a = sr.ReadBlock(buffer, 0, buffersize)) > 0)
                    {
                        if (buffer[buffersize - 1] != '\n')
                        {
                            tail = "";
                            for (int i = buffersize - 1; true; i--)
                            {
                                if (buffer[i] == '\n') break;
                                tail = buffer[i].ToString() + tail;
                            }
                        }

                        str = new String(buffer);
                        str = str.Substring(0, str.Length - tail.Length);
                        str = previos_tail + str;
                        previos_tail = tail;

                        if ((pos = str.IndexOf(searchstring)) != -1)
                        {
                            str = str.Substring(pos);
                            String[] entities = str.Split(delimiterstring);


                            foreach (String entity in entities)
                            {
                                if (entity == "") continue;
                                String[] bhere = entity.Split(delimeterspace);
                                if (bhere.Length < 3) continue;
                                n = GetTotal(totalSize, totalView, out totalSize, out totalView, n, entity);
                            }
                            break;

                        }

                    }
                    String thisLine = "";
                    thisLine = sr.ReadLine();


                    while (((thisLine = sr.ReadLine()) != "") && thisLine != null && IsDomain(thisLine, domain))
                    {
                        n = GetTotal(totalSize, totalView, out totalSize, out totalView, n, thisLine);
                        //Console.WriteLine(n);
                    }

                    //buffersize = 40000;    
                    if (Exporter.storageTable.ContainsKey(currentDay))
                    {
                        Exporter.storageTable[currentDay].Add(totalView, totalSize, n);
                    }
                    else 
                    {
                        Exporter.storageTable.Add(currentDay, new StaticInfo(totalView, totalSize, n));
                    }
                    sr.Close();
                    System.IO.File.Delete(file);
                    BunchHelper.outFile--;

                }
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");

            };

            Exporter.ExportStatisticsTable(domain+"aprmay2013");
        }




        public void DoParsing2()
        {
            String thruDirectory = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug";
            StreamWriter tracker;
            String[] viewsFiles;
            DateTime currentDate;
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            Int32 cursor = 0;
            Int32 buffersize = 20000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            Int64 count = 0;
            String shortname;
            String currentDomain = "";
            StreamWriter errors;
            String currentname;
            Int64 workCounter = 0;
            currentDate = BunchHelper.start_;
            String justincase="";
          
            viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            Console.WriteLine("Parsing started!");
       
            //while (currentDate <= BunchHelper.end_) 
            while (BunchHelper.outFile>0 || BunchHelper.rawFile >0)
            {
                
                foreach (String file in viewsFiles)
                {
                startagain:
                    String[] buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];
                    if (!BunchHelper.unwrapped.ContainsKey(shortname)) continue;

                    StreamReader sr = new StreamReader(file, Encoding.ASCII);
                    currentDate = GetCurrentDate(file);
                    DataRow currentRow;
                    String searchstring;
                    cursor = UpdateCursor(-1, currentDate, out currentRow,out searchstring);
                    if (cursor == BunchHelper.datatable.Rows.Count) break;
                    //if (currentRow["domain"].ToString() == "ru") buffersize = 10000;
                    String previos_tail = "";
                    String tail = "";
                    int pos = -1;
                    Int32 pageviews=0;
                    buffersize = 10000;
                    Int64 pagesize=0;
                    int a;
                    String str = "";
                    Console.WriteLine(currentDate.ToString());
                    while ((a=sr.ReadBlock(buffer,0,buffersize))>0)
                    {
                        if (buffer[buffersize - 1] != '\n')
                        {
                            tail = "";
                            for (int i = buffersize - 1; true; i--)
                            {
                                if (buffer[i] == '\n') break;
                                tail = buffer[i].ToString() + tail;
                            }
                        }

                        str = new String(buffer);
                        str = str.Substring(0, str.Length - tail.Length);
                        str = previos_tail + str;
                        previos_tail = tail;
                       
                    again:
                   

                        if ((String.CompareOrdinal(str.Substring(0, searchstring.Length),searchstring) > 0) && (str.Substring(0,3) == searchstring.Substring(0,3)))
                        {
                            count++;
                            ToResultTable(count, currentRow, currentDate, 0, BunchHelper.pagesizes[Int64.Parse(currentRow["index"].ToString())]);
                            //Console.WriteLine(searchstring + " " + currentDate.ToString() + " " +  cursor.ToString());
                            tracker = new StreamWriter("out.txt",true);
                            tracker.WriteLine(currentRow["index"].ToString() + " " + currentRow["fullname"].ToString()  + " " + searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                            tracker.Close();
                            cursor = UpdateCursor(cursor, currentDate, out currentRow, out searchstring);
                            if (cursor == BunchHelper.datatable.Rows.Count) break;
                            if (!((String.CompareOrdinal(str.Substring(0, searchstring.Length),searchstring) > 0) && (str.Substring(0,3) == searchstring.Substring(0,3))))
                            {
                                str = justincase;
                            }
                            //goto again;

                        }


                        if ((pos = str.IndexOf(searchstring, StringComparison.InvariantCultureIgnoreCase)) != -1)
                         {
                             GetViewStat(str, searchstring, pos, out pagesize, out pageviews);
                             count++;
                             justincase = "";
                             ToResultTable(count, currentRow, currentDate, pageviews, pagesize);
                             //Console.WriteLine(searchstring + " " + currentDate.ToString() + " " +  cursor.ToString());
                             tracker = new StreamWriter("out.txt", true);
                             tracker.WriteLine(currentRow["index"].ToString() + " " + currentRow["fullname"].ToString() + " " + searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                             tracker.Close();
                             BunchHelper.pagesizes[Int64.Parse(currentRow["index"].ToString())] = pagesize;
                             cursor = UpdateCursor(cursor, currentDate, out currentRow, out searchstring);
                             
                             if (cursor == BunchHelper.datatable.Rows.Count) break;
                             goto again;
                         }

                        justincase = str;
                       

                    }
                    
                    //buffersize = 40000;                    
                    sr.Close();
                    System.IO.File.Delete(file);
                    Exporter.Export();
                    BunchHelper.outFile--;
                }
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
 
            } ;

        }

        public void DoParsing4()
        {
            String thruDirectory = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug";
            StreamWriter tracker;
            String[] viewsFiles;
            DateTime currentDate;
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            Int32 cursor = 0;
            Int32 buffersize = 20000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            Int64 count = 0;
            String shortname;
            String currentDomain = "";
            StreamWriter errors;
            String currentname;
            Int64 workCounter = 0;
            currentDate = BunchHelper.start_;
            String justincase = "";

            viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            Console.WriteLine("Parsing started!");

            //while (currentDate <= BunchHelper.end_) 
            while (BunchHelper.outFile > 0 || BunchHelper.rawFile > 0)
            {

                foreach (String file in viewsFiles)
                {
                startagain:
                    String[] buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];
                    if (!BunchHelper.unwrapped.ContainsKey(shortname)) continue;

                    StreamReader sr = new StreamReader(file, Encoding.ASCII);
                    currentDate = GetCurrentDate(file);
                    DataRow currentRow;
                    String searchstring;
                    cursor = UpdateCursor(-1, currentDate, out currentRow, out searchstring);
                    if (cursor == BunchHelper.datatable.Rows.Count) break;
                    //if (currentRow["domain"].ToString() == "ru") buffersize = 10000;
                    String previos_tail = "";
                    String tail = "";
                    int pos = -1;
                    Int32 pageviews = 0;
                    buffersize = 10000;
                    Int64 pagesize = 0;
                    int a;
                    String str = "";
                    Console.WriteLine(currentDate.ToString());
                    while ((a = sr.ReadBlock(buffer, 0, buffersize)) > 0)
                    {
                        if (buffer[buffersize - 1] != '\n')
                        {
                            tail = "";
                            for (int i = buffersize - 1; true; i--)
                            {
                                if (buffer[i] == '\n') break;
                                tail = buffer[i].ToString() + tail;
                            }
                        }

                        str = new String(buffer);
                        str = str.Substring(0, str.Length - tail.Length);
                        str = previos_tail + str;
                        previos_tail = tail;

                    again:


                        if ((String.CompareOrdinal(str.Substring(0, searchstring.Length), searchstring) > 0) && (str.Substring(0, 3) == searchstring.Substring(0, 3)))
                        {
                            count++;
                            ToResultTable(count, currentRow, currentDate, 0, BunchHelper.pagesizes[Int64.Parse(currentRow["index"].ToString())]);
                            //Console.WriteLine(searchstring + " " + currentDate.ToString() + " " +  cursor.ToString());
                            tracker = new StreamWriter("out.txt", true);
                            tracker.WriteLine(currentRow["index"].ToString() + " " + currentRow["fullname"].ToString() + " " + searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                            tracker.Close();
                            cursor = UpdateCursor(cursor, currentDate, out currentRow, out searchstring);
                            if (cursor == BunchHelper.datatable.Rows.Count) break;
                            if (!((String.CompareOrdinal(str.Substring(0, searchstring.Length), searchstring) > 0) && (str.Substring(0, 3) == searchstring.Substring(0, 3))))
                            {
                                str = justincase;
                            }
                            //goto again;

                        }


                        if ((pos = str.IndexOf(searchstring, StringComparison.InvariantCultureIgnoreCase)) != -1)
                        {
                            GetViewStat(str, searchstring, pos, out pagesize, out pageviews);
                            count++;
                            justincase = "";
                            ToResultTable(count, currentRow, currentDate, pageviews, pagesize);
                            //Console.WriteLine(searchstring + " " + currentDate.ToString() + " " +  cursor.ToString());
                            tracker = new StreamWriter("out.txt", true);
                            tracker.WriteLine(currentRow["index"].ToString() + " " + currentRow["fullname"].ToString() + " " + searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                            tracker.Close();
                            BunchHelper.pagesizes[Int64.Parse(currentRow["index"].ToString())] = pagesize;
                            cursor = UpdateCursor(cursor, currentDate, out currentRow, out searchstring);

                            if (cursor == BunchHelper.datatable.Rows.Count) break;
                            goto again;
                        }

                        justincase = str;


                    }

                    //buffersize = 40000;                    
                    sr.Close();
                    System.IO.File.Delete(file);
                    Exporter.Export();
                    BunchHelper.outFile--;
                }
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");

            };

        }

        public void DoParsing3()
        {
            String thruDirectory = @"d:\Users\Lenchick\Google Drive\WASHU FALL2015\play ground\wikiParserRealDigger\wikiParserRealDigger\bin\Debug";
            StreamWriter tracker;
            String[] viewsFiles;
            DateTime currentDate;
            char[] delimiterLast = { '-' };
            char[] delimitersmall = { '\\' };
            Int32 cursor = 0;
            Int32 buffersize = 20000;//4095 * 256;// *16;
            char[] buffer = new char[buffersize];
            Int64 count = 0;
            String shortname;
            String currentDomain = "";
            StreamWriter errors;
            String currentname;
            Int64 workCounter = 0;
            currentDate = BunchHelper.start_;
            String justincase = "";

            viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");
            Console.WriteLine("Parsing started!");

            //while (currentDate <= BunchHelper.end_) 
            while (BunchHelper.outFile > 0 || BunchHelper.rawFile > 0)
            {

                foreach (String file in viewsFiles)
                {
                startagain:
                    String[] buf = file.Split(delimitersmall);
                    shortname = buf[buf.Length - 1];
                    if (!BunchHelper.unwrapped.ContainsKey(shortname)) continue;

                    StreamReader sr = new StreamReader(file, Encoding.ASCII);
                    currentDate = GetCurrentDate2(file);
                    DataRow currentRow;
                    String searchstring;
                    cursor = UpdateCursor(-1, currentDate, out currentRow, out searchstring);
                    if (cursor == BunchHelper.datatable.Rows.Count) break;
                    //if (currentRow["domain"].ToString() == "ru") buffersize = 10000;
                    String previos_tail = "";
                    String tail = "";
                    int pos = -1;
                    Int32 pageviews = 0;
                    buffersize = 10000;
                    Int64 pagesize = 0;
                    int a;
                    String str = "";
                    Console.WriteLine(currentDate.ToString());
                    while ((a = sr.ReadBlock(buffer, 0, buffersize)) > 0)
                    {
                        if (buffer[buffersize - 1] != '\n')
                        {
                            tail = "";
                            for (int i = buffersize - 1; true; i--)
                            {
                                if (buffer[i] == '\n') break;
                                tail = buffer[i].ToString() + tail;
                            }
                        }

                        str = new String(buffer);
                        str = str.Substring(0, str.Length - tail.Length);
                        str = previos_tail + str;
                        previos_tail = tail;

                    again:


                        if ((String.CompareOrdinal(str.Substring(0, searchstring.Length), searchstring) > 0) && (str.Substring(0, 3) == searchstring.Substring(0, 3)))
                        {
                            count++;
                            ToResultTable(count, currentRow, currentDate, 0, BunchHelper.pagesizes[Int64.Parse(currentRow["index"].ToString())]);
                            Console.WriteLine(searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                            tracker = new StreamWriter("out.txt", true);
                            tracker.WriteLine(currentRow["index"].ToString() + " " + currentRow["fullname"].ToString() + " " + searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                            tracker.Close();
                            cursor = UpdateCursor(cursor, currentDate, out currentRow, out searchstring);
                            if (cursor == BunchHelper.datatable.Rows.Count) break;
                            if (!((String.CompareOrdinal(str.Substring(0, searchstring.Length), searchstring) > 0) && (str.Substring(0, 3) == searchstring.Substring(0, 3))))
                            {
                                str = justincase;
                            }
                            //goto again;

                        }


                        if ((pos = str.IndexOf(searchstring, StringComparison.InvariantCultureIgnoreCase)) != -1)
                        {
                            GetViewStat(str, searchstring, pos, out pagesize, out pageviews);
                            count++;
                            justincase = "";
                            ToResultTable(count, currentRow, currentDate, pageviews, pagesize);
                            Console.WriteLine(searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                            tracker = new StreamWriter("out.txt", true);
                            tracker.WriteLine(currentRow["index"].ToString() + " " + currentRow["fullname"].ToString() + " " + searchstring + " " + currentDate.ToString() + " " + cursor.ToString());
                            tracker.Close();
                            BunchHelper.pagesizes[Int64.Parse(currentRow["index"].ToString())] = pagesize;
                            cursor = UpdateCursor(cursor, currentDate, out currentRow, out searchstring);

                            if (cursor == BunchHelper.datatable.Rows.Count) break;
                            goto again;
                        }

                        justincase = str;


                    }

                    //buffersize = 40000;                    
                    sr.Close();
                    System.IO.File.Delete(file);
                    Exporter.Export();
                    BunchHelper.outFile--;
                }
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.out");

            };

        }


        void ToResultTable(Int64 count, DataRow currentRow, DateTime currentDate, Int32 pageviews, Int64 pagesize)
        {
            BunchHelper.resulttable.Rows.Add(count, currentRow["ID"], currentRow["groupID"], currentRow["groupName"],
                                             currentRow["output"],
                                            currentRow["domain"], currentRow["name"], currentRow["fullname"],currentDate.Year.ToString(),
                                            currentDate.Month.ToString(), currentDate.Day.ToString(),
                                            pageviews, pagesize, -1, -1, "", "", "");
        }

    }
}
