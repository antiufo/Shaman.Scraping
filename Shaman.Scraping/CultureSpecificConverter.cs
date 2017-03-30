using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shaman.Runtime
{
    internal class CultureSpecificConverter
    {
        public string At;
        public string Today;
        public string Yesterday;
        public string Tomorrow;
        public string Ago;

        public string[] SinglrTimeIntervals;
        public string[] PluralTimeIntervals;
        public string[] WordsToStrip;
        public string DaySuffixes;

        public string shortMonthNamesRegex;
        

        internal bool MatchesTimeInterval(string str, int index)
        {
            return
                MatchesTimeInterval(str, SinglrTimeIntervals[index]) ||
                MatchesTimeInterval(str, PluralTimeIntervals[index]);
        }

        internal bool MatchesTimeInterval(string str, string interval)
        {
            if (string.IsNullOrEmpty(interval)) return false;
            if (interval.Contains('|')) return interval.SplitFast('|').Contains(str);
            return interval == str;
        }
    }
}
