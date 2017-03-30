using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shaman.Runtime;

namespace System.Globalization
{
    class CultureInfoEx
    {

        [ThreadStatic]
        private static Dictionary<string, CultureInfo> cultures;
        public static CultureInfo GetCultureInfo(string culture)
        {
            if (cultures == null) cultures = new Dictionary<string, CultureInfo>();
            CultureInfo c;
            if (!cultures.TryGetValue(culture, out c))
            {
                c = new CultureInfo(culture);
                cultures[culture] = c;
            }
            return c;
        }
    }
}
