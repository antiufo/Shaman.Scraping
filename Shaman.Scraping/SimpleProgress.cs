using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shaman.Runtime
{
    public struct SimpleProgress
    {
        public int Done { get; set; }
        public int Total { get; set; }
        public string Description { get; set; }
        public SimpleProgress(int done, int total)
        {
            this.Done = done;
            this.Total = total;
            this.Description = null;
        }
        public SimpleProgress(double ratioCompleted)
        {
            this.Done = (int)(ratioCompleted * 100);
            this.Total = 100;
            this.Description = null;
        }

        public SimpleProgress(string description)
        {
            this.Done = 0;
            this.Total = -1;
            this.Description = description;
        }
    }
}
