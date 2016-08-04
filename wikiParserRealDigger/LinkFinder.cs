using System.Collections.Generic;
using System.Text.RegularExpressions;
using HtmlAgilityPack;
using System.Net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;


namespace vpUltimate
{
    public struct LinkItem
    {
        public string Href;
        public string Text;

        public override string ToString()
        {
            return Href + "\n\t" + Text;
        }
    }

    static class LinkFinder
    {
        public static List<LinkItem> Find(string file)
        {
            List<LinkItem> list = new List<LinkItem>();

            // 1.
            // Find all matches in file.
            MatchCollection m1 = Regex.Matches(file, @"(<a.*?>.*?</a>)",
                RegexOptions.Singleline);

            // 2.
            // Loop over each match.
            foreach (Match m in m1)
            {
                string value = m.Groups[1].Value;
                LinkItem i = new LinkItem();

                // 3.
                // Get href attribute.
                Match m2 = Regex.Match(value, @"href=\""(.*?)\""",
                RegexOptions.Singleline);
                if (m2.Success)
                {
                    i.Href = m2.Groups[1].Value;
                }

                // 4.
                // Remove inner tags from text.
                string t = Regex.Replace(value, @"\s*<.*?>\s*", "",
                RegexOptions.Singleline);
                i.Text = t;
                if (t.Substring(t.Length-2,2)=="gz") list.Add(i);
            }
            return list;
        }

        public static string GetWebData(string url)
        {
            string html = string.Empty;
            using (WebClient client = new WebClient())
            {
                Uri innUri = null;
                Uri.TryCreate(url, UriKind.Absolute, out innUri);


                try
                {
                    //client.Headers.Add("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705;)");
                    //using (StreamReader str = new StreamReader(client.OpenRead(innUri)))

                    using (StreamReader str = new StreamReader(client.OpenRead(innUri)))
                    {
                        html = str.ReadToEnd();
                    }
                }
                catch (WebException we)
                {
                    //throw we;"
                    Console.WriteLine(we.ToString());

                }
                return html;
            }
        }

        public static List<LinkItem> Find2(string file)
        {
            List<LinkItem> list = new List<LinkItem>();

            // 1.
            // Find all matches in file.

            HtmlDocument htmldoc = new HtmlDocument();
            htmldoc.LoadHtml(GetWebData(file)); ;

            HtmlNodeCollection nodes = htmldoc.DocumentNode.SelectNodes("//ul");
            nodes = nodes[0].ChildNodes;




            return list;
        }
    }

}
