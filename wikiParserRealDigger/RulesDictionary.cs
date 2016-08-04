using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace vpUltimate
{
    public static class RulesDictionary
    {
        public static List<ComplexPageViewTracker> selectedList;
        //public static Dictionary<String, ComplexPageViewTracker> selectedList;
        public static Dictionary<String, List<int>> pageList;

        public static void NulifyList()
        {
            selectedList = new List<ComplexPageViewTracker>();
            for (int i = 0; i < 12; i++) selectedList.Add(new ComplexPageViewTracker());
        }
        public static void CreateSelectedList(String file)
        {
            //selectedList = new Dictionary<string, ComplexPageViewTracker>();
            //String[] fields = { "name100", "name101", "name102", "category100", "category101", "category102", "name3000", "name3001", "name3002", "category3000", "category3001", "category3002" };

            //foreach (String s in fields)  selectedList.Add(s, new ComplexPageViewTracker());
            selectedList = new List<ComplexPageViewTracker>();
            for (int i = 0; i < 12; i++) selectedList.Add(new ComplexPageViewTracker());


                //name100	name101	name102	category100	category101	category102	name3000	name3001	name3002	category3000	category3001	category3002

            pageList = new Dictionary<string, List<int>>();
            StreamReader sr = new StreamReader(file);

            sr.ReadLine();

            String str; 

            while((str=sr.ReadLine())!=null)
            {
                String[] items = str.Split('\t');
                pageList.Add(items[1], new List<int>());
                for (int i = 2; i < items.Length; i++)
                {
                    pageList[items[1]].Add(Int32.Parse(items[i]));
                }

            }
        }


    }
}
