using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace vpUltimate
{
    public class ArticleDate
    {
        public DateTime time;
        public ArticleDate(String year, String month, String day)
        {
            time = new DateTime(Int16.Parse(year), Int16.Parse(month), Int16.Parse(day));
        }

        public ArticleDate(DateTime dt)
        {
            time = dt;
        }

        public static ArticleDate operator ++(ArticleDate item)
        {
            item.time = new DateTime(item.time.Ticks + TimeSpan.TicksPerHour);
            return item;
        }

        public static Boolean operator >(ArticleDate item1, ArticleDate item2)
        {
            if (item1.time.Ticks > item2.time.Ticks) return true;
            return false;
        }

        public static Boolean operator <(ArticleDate item1, ArticleDate item2)
        {
            if (item1.time.Ticks < item2.time.Ticks) return true;
            return false;
        }

        public DateTime PlusDay()
        {
            return (new DateTime(time.Ticks + TimeSpan.TicksPerDay));
        }

        public DateTime MinusDay()
        {
            return (new DateTime(time.Ticks - TimeSpan.TicksPerDay));
        }

        public String PrintToCsv()
        {
            return (time.Year + "," + time.Month + "," + time.Day + "," + time.Hour);
        }

        public long GetTicks()
        {
            return (time.Ticks);
        }
    }
}
