using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;

namespace vpUltimate
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
            String viewDirectory = @"d:\Users\Lenchick\Google Drive\WASHU FALL2015\play ground\pipe\test\";
            String[] viewsFiles = System.IO.Directory.GetFiles(viewDirectory, "*_in.csv");
            BunchHelper.Start = @"01/2012";
            BunchHelper.End = @"06/2012";

            /*foreach (String file in viewsFiles)
            {
                BunchHelper helper = new BunchHelper();
                helper.processPipeFile(file);
            }*/
            /*
            BunchHelper helper = new BunchHelper();
            helper.processPipeFile(viewsFiles);
            */
            //Run(@"01/2011", @"12/2011");
            //Run(@"04/2012", @"06/2012");
            //Run(@"01/2011", @"12/2011");
            //Run(@"01/2012", @"12/2012");
            //Run(@"01/2013", @"12/2013");
           
            //Run(@"01/2014", @"12/2014");
            Run(@"05/2015", @"04/2016");
            //Run(@"01/2015", @"11/2015","fr");
            //Run(@"01/2015", @"11/2015","ar");
            //UnWrapper.Execute("shutdown /s /t 120");
            //Run(@"01/2013", @"05/2013");
            //*/

            //Clean_wrapper();

        }


        static void Run(String start, String finish, String dd="")
        {
            String viewDirectory = @"d:\wp\pipe\graph\";
            String[] viewsFiles = System.IO.Directory.GetFiles(viewDirectory, "*_in.csv");
            BunchHelper.Start = start;
            BunchHelper.End = finish;

            BunchHelper helper = new BunchHelper();
            helper.processPipeFile(viewsFiles);
            //UnWrapper.Execute("shutdown /s /t 120");
        }

        static void Clean_wrapper()
        {
            //Clean("*_2012.csv");
            //Clean("*_2013.csv");
            Clean("*_2015.csv");
        }
        static void Clean(String filter)
        {
            String viewDirectory = @"d:\Users\Lenchick\Google Drive\WASHU FALL2015\play ground\pipe\_pageviews\";
            String[] viewsFiles = System.IO.Directory.GetFiles(viewDirectory,filter);

            foreach (String file in viewsFiles)
            {
                System.IO.File.Delete(file);
            }

        }
    }
}
