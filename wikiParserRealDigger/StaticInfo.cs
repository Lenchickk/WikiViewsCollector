using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace vpUltimate
{
    public class StaticInfo
    {
        public Int64 views;
        public Double megabytes;
        public Int64 n;
        const Double norm =0;

        public StaticInfo(Int64 v, Double b, Int64 nEx)
        {
            views = v;
            megabytes = b;
            n = nEx;
        }

        public void Add(Int64 v, Double b, Int64 nEx)
        {
            views+= v;
            megabytes += b;
            n+= nEx;
        }
    }
}
