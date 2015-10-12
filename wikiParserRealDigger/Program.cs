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

            String viewDirectory = @"C:\Users\Lenchick\Google Drive\WASHU FALL2015\play ground\pipe\test\";
            String[] viewsFiles = System.IO.Directory.GetFiles(viewDirectory, "*_in.csv");
            BunchHelper.Start = @"01/2011";
            BunchHelper.End = @"09/2015";

            foreach (String file in viewsFiles)
            {
                BunchHelper helper = new BunchHelper();
                helper.processPipeFile(file);
            }


        }
    }
}
