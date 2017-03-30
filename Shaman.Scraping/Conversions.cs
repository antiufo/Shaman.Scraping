using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Drawing;
using System.Reflection;
#if SHAMAN
using Windows.Foundation;
using Shaman.Runtime;
using Shaman.Annotations;
#endif
using Shaman.Types;
using Shaman.Dom;
using System.Linq.Expressions;

namespace Shaman.Runtime
{
    /// <summary>
    /// Provides conversions functions for formatted values.
    /// </summary>
    public static partial class Conversions
    {


        /// <summary>
        /// Parses a date/time from a display string.
        /// Friendly values like "Yesterday at 5 PM" are supported as well.
        /// </summary>
        /// <param name="s">The string to parse.</param>
        /// <param name="culture">The culture for date rules.</param>
        /// <returns>The parsed date/time</returns>
        public static DateTime ParseDateTime(string s, CultureInfo culture, DateTime? referenceTime, string timezone = null, string format = null)
        {

            var date = ParseDateTimeInternal(s, culture, true, referenceTime.GetValueOrDefault(DateTime.UtcNow), false, timezone, format);
            if (date == null) throw new FormatException("Date cannot be parsed.");
            return date.Value.UtcDateTime;
        }

        [Configuration]
        public static readonly int Configuration_UnixMinAllowedYear = 1990;
        [Configuration]
        public static readonly int Configuration_UnixMaxAllowedYear = 2050;

        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        private static readonly char[] Colon = new char[] { ':' };

        private readonly static DateTimeStyles DateTimeFlags = DateTimeStyles.None | DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces;

        [AllowNumericLiterals]
        private static DateTimeOffset? ParseDateTimeInternal(string s, CultureInfo culture, bool allowUnixTimestamp, DateTime referenceTime, bool nullOnFailure, string timezone = null, string format = null)
        {
            if (string.IsNullOrWhiteSpace(s) || (allowUnixTimestamp && s == "0" || s == "-1")) return null;

            if (int.TryParse(s, out var year))
            {
                if (year < 2050 && year >= 1940)
                    return new DateTime(year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            }

            TimeZoneInfo tzi = null;
            if (timezone != null)
            {
#if DESKTOP
                tzi = TimeZoneInfo.FindSystemTimeZoneById(timezone);
#else
                throw new NotSupportedException("Time zone info not available on this platform.");
#endif
            }

            if (allowUnixTimestamp)
            {
                long value;
                if (long.TryParse(s, out value))
                {
                    var asMilliseconds = UnixEpoch.AddMilliseconds(value);
                    var unixMinDate = new DateTime(Configuration_UnixMinAllowedYear, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
                    var unixMaxDate = new DateTime(Configuration_UnixMaxAllowedYear, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
                    if (asMilliseconds > unixMinDate && asMilliseconds < unixMaxDate) return AdjustForTimeZone(asMilliseconds, tzi);

                    var asSeconds = UnixEpoch.AddSeconds(value);
                    if (asSeconds > unixMinDate && asSeconds < unixMaxDate) return AdjustForTimeZone(asSeconds, tzi);

                    if (nullOnFailure) return null;
                    throw new ArgumentException("Ambiguous Unix timestamp, can't tell if specified in seconds or milliseconds.");
                }
            }

            var converter = EnglishConverter;
            if (culture == null) culture = CultureInfo.InvariantCulture;
            var langcode = culture.TwoLetterISOLanguageName;
            if (langcode == "sv") converter = SwedishConverter;
            else if (langcode == "da") converter = DanishConverter;
            else if (langcode == "nl") converter = DutchConverter;
            else if (langcode == "es") converter = SpanishConverter;
            else if (langcode == "it") converter = ItalianConverter;
            else if (langcode == "de") converter = GermanConverter;


            if (converter.shortMonthNamesRegex == null)
            {
                converter.shortMonthNamesRegex = string.Join("|", Enumerable.Range(1, 12).Select(x => new DateTime(2000, x, 15, 0, 0, 0, DateTimeKind.Utc).ToString("MMM", converter == EnglishConverter ? CultureInfo.InvariantCulture : GetCulture(langcode))));
            }

            s = s.Replace("a.m.", "am");
            s = s.Replace("p.m.", "pm");

            DateTime d;
            var hasYear = Regex.IsMatch(s, @"\b(?:1|2)\d\d\d\b");
            if (s.EndsWith("UTC")) s = s.Substring(0, s.Length - 3) + "GMT";
            if (format != null)
            {
                if (DateTime_TryParseExact(s, format, culture, out d, referenceTime))
                    return AdjustForTimeZone(d, tzi);
            }
            else
            {
                bool parsedDateHasYear;
                if (DateTime_TryParse(s, culture, out d, referenceTime, out parsedDateHasYear))
                {
                    var skip = false;
                    if (!parsedDateHasYear)
                    {
                        if (Regex.IsMatch(s, converter.shortMonthNamesRegex, RegexOptions.IgnoreCase)) skip = true;
                    }
                    if(!skip)
                        return AdjustForTimeZone(d, tzi);
                }
            }
            s = s.ToLowerFast();

            var words = s.Split(new[] { " ", ",", "\r", "\n", "\t", "utc" }, StringSplitOptions.RemoveEmptyEntries);

            if (words.Length == 0) return null;

            words = words.Where(x => !converter.WordsToStrip.Contains(x.Trim(Colon))).ToArray();
            if (words.Length == 0) return null;
            s = s.RegexReplace(@"\b(" + string.Join("|", converter.WordsToStrip) + @")\b", string.Empty).Trim();
            if (words.Last() == converter.Ago || words[0] == converter.Ago)
            {

                var interval = TimeSpan.Zero;
                for (int i = 0; i < words.Length - 1; i += 2)
                {
                    if (words[i] == converter.Ago) continue;
                    var num = int.Parse(words[i]);
                    var unit = words[i + 1];
                    TimeSpan increment;
                    if (converter.MatchesTimeInterval(unit, 0)) increment = TimeSpan.FromDays(365 * num);
                    else if (converter.MatchesTimeInterval(unit, 1)) increment = TimeSpan.FromDays(30 * num);
                    else if (converter.MatchesTimeInterval(unit, 2)) increment = TimeSpan.FromDays(7 * num);
                    else if (converter.MatchesTimeInterval(unit, 3)) increment = TimeSpan.FromDays(num);
                    else if (converter.MatchesTimeInterval(unit, 4)) increment = TimeSpan.FromHours(num);
                    else if (converter.MatchesTimeInterval(unit, 5)) increment = TimeSpan.FromMinutes(num);
                    else if (converter.MatchesTimeInterval(unit, 6)) increment = TimeSpan.FromSeconds(num);
                    else if (nullOnFailure) return null;
                    else throw new ArgumentException("DateTime cannot be parsed.");
                    interval += increment;
                }
                return referenceTime - interval;
            }
            else
            {
                var offset = tzi != null ? tzi.GetUtcOffset(DateTime.Today) : TimeSpan.Zero;
                var atregex = @"\b" + converter.At + @"\b";
                if (s.StartsWith(converter.Today))
                {
                    d = DateTime.UtcNow.Date;
                    s = s.Substring(converter.Today.Length);
                }
                else if (s.StartsWith(converter.Yesterday))
                {
                    d = DateTime.UtcNow.Date.AddDays(-1);
                    s = s.Substring(converter.Yesterday.Length);
                }
                else if (s.StartsWith(converter.Tomorrow))
                {
                    d = DateTime.UtcNow.Date.AddDays(1);
                    s = s.Substring(converter.Tomorrow.Length);
                }
                else
                {
                    if (!s.Any(x => char.IsDigit(x))) return null;
                    s = s.RegexReplace(atregex, string.Empty);
                    if (converter.DaySuffixes != null)
                    {
                        s = s.RegexReplace(@"(?<=\d)(?:" + converter.DaySuffixes + @")\b", string.Empty);
                    }
                    if (format != null)
                    {
                        if (!DateTime_TryParseExact(s, format, culture, out d, referenceTime)) return null;
                    }
                    else
                    {

                        if (!hasYear)
                        {
                           


                            s = Regex.Replace(s, @"
\b
(
    (?:
        (?:" + converter.shortMonthNamesRegex + @")\w*\s+ |
        \d{1,2}\s*[/\-]\s*
    )
    \d{1,2}
)\b", x => x.Groups[1] + ", " + referenceTime.Year.ToString(CultureInfo.InvariantCulture), RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);

                        }
                        if (!DateTime_TryParse(s, culture, out d, referenceTime, out var dummy)) return null;

                    }
                    s = null;
                }
                d += offset;

                s = s != null ? s.RegexReplace(atregex, string.Empty).Trim(new[] { ' ', ',' }) : null;
                
                if (!string.IsNullOrEmpty(s))
                {

                    bool? pm = null;
                    var amDesignator = culture.DateTimeFormat.AMDesignator;
                    var pmDesignator = culture.DateTimeFormat.PMDesignator;
                    if (string.IsNullOrEmpty(amDesignator)) amDesignator = "am";
                    if (string.IsNullOrEmpty(pmDesignator)) pmDesignator = "pm";
                    if (s.EndsWith(amDesignator, StringComparison.OrdinalIgnoreCase))
                    {
                        s = s.Substring(0, s.Length - amDesignator.Length).Trim();
                        pm = false;
                    }
                    else if (s.EndsWith(pmDesignator, StringComparison.OrdinalIgnoreCase))
                    {
                        s = s.Substring(0, s.Length - pmDesignator.Length).Trim();
                        pm = true;
                    }

                    TimeSpan interval;
                    if (!TimeSpan.TryParse(s, culture, out interval))
                    {
                        if (!TimeSpan.TryParseExact(s, @"h\.m", culture, out interval)) return null;
                    }
                    if (interval.Hours != 12 && pm == true) interval += TimeSpan.FromHours(12);
                    if (interval.Hours == 12 && pm == false) interval -= TimeSpan.FromHours(12);
                    d += interval;
                }
                return d; // Already adjusted
            }
        }

        
        private static MethodInfo DateTimeParse_TryParse;
        private static Type DateTimeResultType;
        private static Func<object> CreateDateTimeResult;
        private static Func<object, object> CreateDateTimeResult_getParseFlags;
        private static Func<object, DateTime> DateTimeResult_getParsedDate;
        private static bool DateTime_TryParse(string s, IFormatProvider provider, out DateTime result, DateTime referenceTime, out bool hasYear)
        {
            result = DateTime.MinValue;

            if (CreateDateTimeResult == null)
            {

                var t = typeof(DateTime)
#if SHAMAN
                    .Assembly()
#else
                    .GetTypeInfo().Assembly
#endif
                    .GetType("System.DateTimeParse");
                DateTimeResultType = typeof(DateTime)
#if SHAMAN
                    .Assembly()
#else
                    .GetTypeInfo().Assembly
#endif
                    .GetType("System.DateTimeResult");
                DateTimeParse_TryParse = t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static).Single(x => x.Name == "TryParse" && x.GetParameters().Last().ParameterType.FullName == "System.DateTimeResult&");
                var p = Expression.Variable(DateTimeResultType, "r");
                //var s_param = Expression.Parameter(typeof(string), "s");
                //var dtfi_param = Expression.Parameter(typeof(DateTimeFormatInfo), "dtfi");
                var fields = DateTimeResultType.GetRuntimeFields();
                CreateDateTimeResult_getParseFlags = (Func<object, object>)ReflectionHelper.GetGetter(fields.First(x => x.Name=="flags"), typeof(Func<object, object>));
                DateTimeResult_getParsedDate = (Func<object, DateTime>)ReflectionHelper.GetGetter(fields.First(x => x.Name == "parsedDate"), typeof(Func<object, DateTime>));
                CreateDateTimeResult = (Func<object>)Expression.Lambda<Func<object>>(Expression.Block(new[] { p },
                    Expression.Assign(p,  Expression.New(DateTimeResultType)),
                    Expression.Call(p, DateTimeResultType.GetMethod("Init", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)),
                    //Expression.Call(p, DateTimeParse_TryParse, s_param, dtfi_param, Expression.Constant(DateTimeFlags), ),
                    Expression.Convert(p, typeof(object)))
                    //s_param, dtfi_param
                    ).Compile();
            }

            var dateTimeResult = CreateDateTimeResult();

            var arr = new object[] { s, DateTimeFormatInfo.GetInstance(provider), DateTimeFlags, dateTimeResult };
            // BUG: Parse("feb 29", refdate 2016) does not work if current year is not a leap year
            hasYear = false;
            if ((bool)DateTimeParse_TryParse.Invoke(null, arr))
            {    
                dateTimeResult = arr[3];
                var parsedDate = DateTimeResult_getParsedDate(dateTimeResult);
                var flags = (ParseFlags)Convert.ToInt32(CreateDateTimeResult_getParseFlags(dateTimeResult));

                var year = parsedDate.Year;
                var month = parsedDate.Month;
                var day = parsedDate.Day;
                var hour = parsedDate.Hour;
                var minute = parsedDate.Minute;
                var second = parsedDate.Second;
                var millisecond = parsedDate.Millisecond;

                hasYear = true;
                if ((flags & ParseFlags.YearDefault) != 0)
                {
                    year = referenceTime.Year;
                    hasYear = false;
                }
                

                if ((flags & ParseFlags.HaveDate) == 0)
                {
                    year = referenceTime.Year;
                    month = referenceTime.Month;
                    day = referenceTime.Day;
                    hasYear = false;
                }

                result = new DateTime(year, month, day, hour, minute, second, millisecond, DateTimeKind.Utc);

                return true;
            }
            return false;

        }

        [Flags]
        internal enum ParseFlags
        {
            HaveYear = 1,
            HaveMonth = 2,
            HaveDay = 4,
            HaveHour = 8,
            HaveMinute = 16,
            HaveSecond = 32,
            HaveTime = 64,
            HaveDate = 128,
            TimeZoneUsed = 256,
            TimeZoneUtc = 512,
            ParsedMonthName = 1024,
            CaptureOffset = 2048,
            YearDefault = 4096,
            Rfc1123Pattern = 8192,
            UtcSortPattern = 16384
        }

        private static bool DateTime_TryParseExact(string s, String format, CultureInfo culture, out DateTime d, DateTime referenceTime)
        {
            if (DateTime.TryParseExact(s, format, culture, DateTimeFlags, out d))
            {
                return true;
            }
            return false;
        }

        private static DateTime AdjustForTimeZone(DateTime date, TimeZoneInfo tzi)
        {
            if (tzi == null) return new DateTime(date.Ticks, DateTimeKind.Utc);
            return new DateTime(date.Ticks - tzi.GetUtcOffset(date).Ticks, DateTimeKind.Utc);
        }

#if SHAMAN


        /// <summary>
        /// Converts a string to an object of the specified type.
        /// </summary>
        /// <param name="str">The string to convert.</param>
        /// <typeparam name="T">The destination type.</typeparam>
        /// <param name="culture">The culture identifier for number and dates.</param>
        /// <returns>The converted object.</returns>
        public static T ConvertFromString<T>(string str, string culture = null, string format = null)
        {
            return (T)ConvertFromString(str, TypeBase.FromAnyNativeType<T>(), culture: culture, format: format);
        }

        /// <summary>
        /// Converts a string to an object of the specified type.
        /// </summary>
        /// <param name="str">The string to convert.</param>
        /// <param name="type">The destination type.</param>
        /// <param name="descriptiveName">A descriptive name for the object (if any).</param>
        /// <param name="culture">The culture identifier for number and dates.</param>
        /// <returns>The converted object.</returns>
        public static object ConvertFromString(string str, TypeBase type, string descriptiveName = null, string culture = null, Currency currency = Currency.None, HtmlDocument ownerDocument = null, string inlineListSeparator = null, string timezone = null, string format = null)
        {
            var cult = GetCulture(culture);
            var underlyingType = TypeBase.GetUnderlyingType(type);
            if (underlyingType != null)
                return ConvertFromString(str, underlyingType, descriptiveName, culture, currency, ownerDocument, inlineListSeparator, timezone, format);

            var enumType = type as EnumType;
            if (enumType != null) return enumType.Resolve(str, false);


            var t = type.GetNativeType();



            if (t == typeof(string)) return str;
            if (t == typeof(Html)) return new Html(str, ownerDocument != null ? ownerDocument.GetLazyBaseUrl() : null);
            if (t == typeof(FileSize)) return FileSize.Parse(str);
            if (t == typeof(GeographicLocation)) return GeographicLocation.FromString(str);
            if (t == typeof(Uri)) return new Uri(str);
            if (t == typeof(UriObject)) return UriObject.FromUrl(str.AsUri());
            if (t == typeof(WebImage)) return WebImage.FromUrl(str.AsUri());
            if (t == typeof(WebFile)) return WebFile.FromUrl(str.AsUri());
            if (t == typeof(WebAudio)) return WebAudio.FromUrl(str.AsUri());
            if (t == typeof(WebVideo)) return WebVideo.FromUrl(str.AsUri());

            if (t == typeof(Money))
            {
                if (string.IsNullOrEmpty(str)) return null;
                return Money.Parse(str, currency, cult);
            }

            // if (t == typeof(Memo)) return new Memo(str);

            if (t == typeof(SByte)) return Convert.ToSByte(str);
            if (t == typeof(Int16)) return Convert.ToInt16(RemoveThousandsSeparators(str, cult), cult);
            if (t == typeof(Int32)) return Convert.ToInt32(RemoveThousandsSeparators(str, cult), cult);
            if (t == typeof(Int64)) return Convert.ToInt64(RemoveThousandsSeparators(str, cult), cult);

            if (t == typeof(Byte)) return Convert.ToByte(str);
            if (t == typeof(UInt16)) return Convert.ToUInt16(RemoveThousandsSeparators(str, cult), cult);
            if (t == typeof(UInt32)) return Convert.ToUInt32(RemoveThousandsSeparators(str, cult), cult);
            if (t == typeof(UInt64)) return Convert.ToUInt64(RemoveThousandsSeparators(str, cult), cult);


            if (t == typeof(Single)) return Convert.ToSingle(RemoveThousandsSeparators(str, cult), cult);
            if (t == typeof(Double)) return Convert.ToDouble(RemoveThousandsSeparators(str, cult), cult);
            if (t == typeof(Decimal)) return Convert.ToDecimal(RemoveThousandsSeparators(str, cult), cult);



            if (t == typeof(DateTime)) return ParseDateTime(str, cult, ownerDocument != null ? Utils.TryGetPageRetrievalDate(ownerDocument) : null, timezone, format);
            if (t == typeof(Size)) return new Size(int.Parse(str.Capture(@"(\d+)\s*(?:x|×|\*)")), int.Parse(str.Capture(@"(?:x|×|\*)\s*(\d+)")));
            if (type == TypeBase.BusinessWebsite) return BusinessWebsite.FromString(str);
            if (type is EntityType) return ObjectManager.GetEntityAsync((EntityType)type, str.AsUri(), false, descriptiveName).AssumeCompleted();
            if (t == typeof(TimeSpan)) return ParseTimeSpan(str, cult);
            if (t == typeof(bool))
            {
                var value = ConvertFromBoolean(str, true, cult);
                if (value == null) throw new InvalidDataException(string.Format("String cannot be converted to boolean: \"{0}\"", str));
                return value.Value;
            }

            if (t == typeof(BusinessWebsite)) return BusinessWebsite.TryParse(str);

            var simpleType = type as SimpleType;
            if (simpleType != null && simpleType.InlineListItemType != null)
            {
                return Utils.ParseInlineList(str, simpleType, inlineListSeparator);
            }


            throw new ArgumentException("Type cannot be converted from string: " + t.ToString());


        }
#endif
        /// <summary>
        /// Returns a culture based on its identifier.
        /// </summary>
        /// <param name="culture">The culture identifier, or null for the invariant culture.</param>
        /// <returns>The requested culture.</returns>
        private static CultureInfo GetCulture(string culture)
        {
            if (culture == null) return CultureInfo.InvariantCulture;
#if (!DESKTOP && SHAMAN) || CORECLR
            else return CultureInfoEx.GetCultureInfo(culture);
#else
            else return CultureInfo.GetCultureInfo(culture);
#endif

        }

        private static TimeSpan? ParseTimeSpanInternal(string text, CultureInfo culture)
        {
            if (string.IsNullOrEmpty(text)) return null;
            if (!char.IsDigit(text[0])) return null;
            if (!text.Contains(':')) return null;
            TimeSpan value;

            if (TimeSpan.TryParse("00:" + text, out value)) return value;
            return null;
        }

        /// <summary>
        /// Tries to parse a date/time, returns null if the conversion fails.
        /// </summary>
        /// <param name="text">The text to parse.</param>
        /// <param name="culture">The culture to use.</param>
        /// <returns>The converted date/time, or null.</returns>
        public static TimeSpan? TryParseTimeSpan(string text, CultureInfo culture)
        {
            try
            {
                return ParseTimeSpanInternal(text, culture);
            }
            catch
            {
                return null;
            }
        }


        /// <summary>
        /// Parses a time span.
        /// </summary>
        /// <param name="text">The text to convert.</param>
        /// <param name="culture">The culture to use.</param>
        /// <returns>The parsed time span.</returns>
        public static TimeSpan ParseTimeSpan(string text, CultureInfo culture)
        {
            var t = ParseTimeSpanInternal(text, culture);
            if (t == null) throw new ArgumentException("Cannot parse the TimeSpan value.");
            return t.Value;
        }
#if SHAMAN
        /// <summary>
        /// Returns a textual representation of the object.
        /// </summary>
        /// <param name="value">The object to convert.</param>
        /// <returns>The string representing the object.</returns>
        public static string ConvertToDisplayString(object value)
        {
            if (value == null) throw new ArgumentNullException();
            var t = value.GetType();
            var tinfo = t.GetTypeInfo();

            if (tinfo.IsEnum)
            {
                var enumType = EnumType.FromNativeType(t);
                return enumType.ToString(value).PascalCaseToNormalCase();
            }
            else if (t == typeof(Size))
            {
                var size = (Size)value;
                return String.Format("{0}×{1}", size.Width, size.Height);
            }
            else if (t == typeof(TimeSpan))
            {
                var timespan = (TimeSpan)value;
                if (timespan < TimeSpan.FromMinutes(60)) return timespan.ToString();
                else return timespan.ToString(@"mm\:ss");
            }
            else if (t == typeof(DateTime))
            {
                var diff = DateTime.UtcNow - (DateTime)value;
                if (diff.Ticks > 0) return GetDurationText(diff) + " ago";
                return "in " + GetDurationText(-diff);
            }


            return value.ToString();

        }

        [AllowNumericLiterals]
        private static string GetDurationText(TimeSpan duration)
        {
            if (duration.TotalSeconds < 90) return "1 minute";
            else if (duration.TotalMinutes < 60) return Math.Round(duration.TotalMinutes) + " minutes";
            else if (duration.TotalHours < 1.5) return "1 hour";
            else if (duration.TotalHours < 20) return Math.Round(duration.TotalHours) + " hours";
            else if (duration.TotalHours < 40) return "1 day";
            else if (duration.TotalDays < 30) return Math.Round(duration.TotalDays) + " days";
            else if (duration.TotalDays < 45) return "1 month";
            else if (duration.TotalDays < 365) return Math.Round(duration.TotalDays / 30) + " months";
            else if (duration.TotalDays < 600) return "1 year";
            else return Math.Round(duration.TotalDays / 365) + " years";
        }

        /// <summary>
        /// Converts a URL to an object of the specified type.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <param name="type">The destination type.</param>
        /// <returns>The converted object.</returns>
        public static object ConvertFromUrl(Uri url, TypeBase type)
        {
            return ConvertFromString(url.AbsoluteUri, type);
        }

        private static bool? ConvertFromBoolean(string str, bool allow01, CultureInfo culture)
        {
            if (allow01)
            {
                if (str == "1") return true;
                if (str == "0") return false;
            }

            var lower = str.ToLowerFast();
            if (lower == "yes" || lower == "true" || lower == "y" || lower == "t") return true;
            else if (lower == "no" || lower == "false" || lower == "n" || lower == "f") return false;
            return null;
        }


        /// <summary>
        /// Determines whether the specified string is a valid boolean value.
        /// </summary>
        /// <param name="str">The string.</param>
        /// <param name="culture">The culture to use.</param>
        /// <returns>Whether the string is a valid boolean or not.</returns>
        public static bool IsBoolean(string str, CultureInfo culture)
        {
            return Conversions.ConvertFromBoolean(str, false, culture) != null;
        }


        internal static string RemoveThousandsSeparators(string s, CultureInfo culture)
        {
            s = s.Replace(" ", "");
            var separator = culture.NumberFormat.NumberGroupSeparator;
            if (separator != null) return s.Replace(separator, string.Empty);
            return s;
        }
#endif


        /// <summary>
        /// Tries to parse a date/time, returns null if the conversion fails.
        /// </summary>
        /// <param name="str">The string to convert.</param>
        /// <param name="culture">The culture to use.</param>
        /// <param name="allowUnixTimestamp">Whether unix timestamps should be accepted or not.</param>
        /// <returns></returns>
        public static DateTime? TryParseDateTime(string str, CultureInfo culture, bool allowUnixTimestamp, DateTime? referenceTime)
        {
            try
            {
                var q = ParseDateTimeInternal(str, culture, allowUnixTimestamp, referenceTime.GetValueOrDefault(DateTime.UtcNow), true);
                return q != null ? (DateTime?)q.Value.UtcDateTime : null;
            }
            catch
            {
                return null;
            }
        }


    }
}
