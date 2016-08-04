using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace vpUltimate
{
    public class UnWrapper
    {
        private volatile bool _shouldStop;
        public void RequestStop()
        {
            _shouldStop = true;
        }
        // Volatile is used as hint to the compiler that this data
        // member will be accessed by multiple threads.

        public static int outnumber = 0;

        public void UnWrapperStream()
        {
            String thruDirectory = @"d:\wp\code\3_wikiParserRealDigger\wikiParserRealDigger\bin\Debug\";
            String[] viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.gz");
            //Int32 order;
            char[] delimiterLast = { '\\' };
            String[] helper;
            String shortname = "";

            Console.WriteLine("Unwrapper started" + outnumber.ToString());
            outnumber++;

            while (BunchHelper.tasks.Count>0)
            //while (true)
            {
                 while (BunchHelper.outFile>=5)
                 {
                     ;
                 }
                foreach (string file in viewsFiles)
                {
                    helper = file.Split(delimiterLast);
                    while (BunchHelper.outFile >= 5)
                    {
                        ;
                    }
                    shortname = helper[helper.Length - 1];
                    if (!BunchHelper.downloaded.ContainsKey(shortname)) continue;
                    if (BunchHelper.startedUnwrap.ContainsKey(shortname))
                    {
                        continue;
                    }
                    BunchHelper.startedUnwrap.Add(shortname, true);
                    String outname = shortname + ".out";
                    Execute("gzip -dc  " + shortname + "  >  " + outname + " ");
                    BunchHelper.outFile++;
                    //order=Int32.Parse(file.Split(delimiterOrder)[1]);
                    System.IO.File.Delete(file);
                    BunchHelper.rawFile--;
                    //BunchHelper.unwrapped[order] = true;
                    BunchHelper.unwrapped.Add(outname, true);
                    BunchHelper.wrapstart = true;
                }
                viewsFiles = System.IO.Directory.GetFiles(thruDirectory, "*.gz");

            }

            BunchHelper.unwrapperThreadEnd = true;
            Console.WriteLine("Unwrapper ended");
        }

        static public void Execute(String command)
        {
            System.Diagnostics.ProcessStartInfo procStartInfo = new System.Diagnostics.ProcessStartInfo("cmd", "/c " + command);
            System.Diagnostics.Process proc = new System.Diagnostics.Process();
            proc.StartInfo = procStartInfo;
            proc.Start();
            do { } while (!proc.HasExited);
        }

    }
}
