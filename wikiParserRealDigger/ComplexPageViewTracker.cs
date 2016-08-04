using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace vpUltimate
{
    public class ComplexPageViewTracker
    {
        public Int64 n = 0;
        public Int64 totalView = 0;
        public Double totalSize = 0;


        public ComplexPageViewTracker(Int64 inn = 0, Int64 intotalView = 0, Double intotalSize = 0)
        {
            n = inn;
            totalView = intotalView;
            totalSize = intotalSize;
        }

        public void Add(Int64 inn = 0, Int64 intotalView = 0, Double intotalSize = 0)
        {
            n+= inn;
            totalView+= intotalView;
            totalSize+= intotalSize;
        }
    }
}
