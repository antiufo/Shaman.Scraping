using Shaman.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using Shaman.Dom;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Net.Http;
using Newtonsoft.Json;
using System.Reflection;
#if DOM_EMULATION
using Shaman.DomEmulation;
#endif
using System.Threading;

namespace Shaman.Scraping
{
    public class Program
    {
        public static void Main()
        {
            ConfigurationManager.Initialize(typeof(Program).GetTypeInfo().Assembly, IsDebug);
            ConfigurationManager.Initialize(typeof(HttpUtils).GetTypeInfo().Assembly, IsDebug);
            ConfigurationManager.Initialize(typeof(SingleThreadSynchronizationContext).GetTypeInfo().Assembly, IsDebug);

            try
            {
                Shaman.Runtime.SingleThreadSynchronizationContext.Run(MainAsync);
            }
            finally
            {
                BlobStore.CloseAllForShutdown();
            }
        }


#if DEBUG
        private readonly static bool IsDebug = true;
#else
        private readonly static bool IsDebug = false;
#endif

        [Configuration(CommandLineAlias = "make-cdx")]
        public static string Configuration_MakeCdx;

        [Configuration(CommandLineAlias = "facebook-make-csv")]
        public static bool Configuration_FacebookMakeCsv;


        [Configuration(CommandLineAlias = "cookies")]
        public static string Configuration_Cookies;

        [Configuration(CommandLineAlias = "cookie-file")]
        public static string Configuration_CookieFile;


        public static async Task MainAsync()
        {

            Shaman.Runtime.Tld.GetTldRulesCallback = () => File.ReadAllText(ConfigurationManager.CombineRepositoryOrEntrypointPath("Awdee2.Declarative/effective_tld_names.dat"));
            HtmlDocument.CustomPageUrlTypeConverter = x => ((LazyUri)x).Url;

            if (Configuration_MakeCdx != null)
            {
                WarcCdxItemRaw.GenerateCdx(string.IsNullOrEmpty(Configuration_MakeCdx) ? Directory.GetCurrentDirectory() : Configuration_MakeCdx);
                return;
            }
            var cookies = 
                (Configuration_Cookies ??
                (Configuration_CookieFile != null ? File.ReadAllText(Configuration_CookieFile) : null))?.Trim().TrimStart("Cookie:").Trim();


            var allDynamicParameters = typeof(Program)
                .GetTypeInfo()
                .Assembly
                .GetTypes()
                .Where(x => typeof(WebsiteScraper).IsAssignableFrom(x))
                .SelectMany(x => ((IEnumerable<MemberInfo>)x.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly).Where(y => IsMemberTypeOkForDynamicParameter(y.FieldType))).Concat(x.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly).Where(y => IsMemberTypeOkForDynamicParameter(y.PropertyType))));
            var dynamicParameters = new Dictionary<string, MemberInfo>();
            foreach (var item in allDynamicParameters)
            {
                var name = "--" + Dasherize(item.DeclaringType.Name.TrimEnd("Scraper")) + "-" + Dasherize(item.Name);
                dynamicParameters.Add(name, item);
            }
            //File.WriteAllLines(@"c:\temp\-scraping-extra-parameters.txt", dynamicParameters.Keys.OrderBy(x => x), Encoding.UTF8);

            var positional = Environment.GetCommandLineArgs();



            //Caching.EnableWebCache("/Awdee/Cache/DomEmulation");

            WebsiteScraper commandLineScraper = null;

            for (var i = 0; i < positional.Length; i++)
            {
                if (dynamicParameters.TryGetValue(positional[i], out var member))
                {
                    var t = member.DeclaringType;
                    if (commandLineScraper == null || !(t.IsAssignableFrom(commandLineScraper.GetType())))
                    {
                        commandLineScraper = (WebsiteScraper)Activator.CreateInstance(t);
                        Program.InitScraperDefaults(commandLineScraper);
                        commandLineScraper.Cookies = cookies;
                    }
                    var next = i + 1 < positional.Length ? positional[i + 1] : null;
                    string val = string.Empty;
                    var hasValue = false;
                    if (next != null && !next.StartsWith("--"))
                    {
                        val = next;
                        i++;
                        hasValue = true;
                    }
                    object v = null;
                    var mt = (member as PropertyInfo)?.PropertyType ?? ((FieldInfo)member).FieldType;
                    mt = Nullable.GetUnderlyingType(mt) ?? mt;
                    if (mt == typeof(bool))
                    {
                        if (val.In(string.Empty, "1", "y", "yes", "true")) v = true;
                        else if (val.In("0", "n", "no", "false")) v = false;
                        else throw new Exception("Cannot parse boolean: " + val);
                    }
                    else
                    {
                        if (!hasValue) throw new Exception("Missing value for parameter " + positional[i]);
                        if (mt == typeof(string)) v = val;
                        else if (mt == typeof(double) || mt == typeof(float))
                        {
                            v = Convert.ChangeType(double.Parse(val), mt);
                        }
                        else
                        {
                            v = Convert.ChangeType(decimal.Parse(val), mt);
                        }
                    }

                    if (member is PropertyInfo p) p.SetValue(commandLineScraper, v);
                    else ((FieldInfo)member).SetValue(commandLineScraper, v);
                }
            }


            if (commandLineScraper != null)
            {
                using (commandLineScraper)
                {
                    if (!Configuration_FacebookMakeCsv)
                        await commandLineScraper.ScrapeAsync();
                    if (commandLineScraper is FacebookScraper f)
                    {

                        //f.AddImagesToCrawl();
                        //f.ReconsiderSkippedUrls();
                        //await f.ScrapeAsync();

                        Console.WriteLine("Creating post list");
                        f.CreatePostList();
                    }
                }
                return;
            }
            
#if SCRAPING_SANDBOX
            
            
            using (var scraper = new WebsiteScraper())
            {
                InitScraperDefaults(scraper);/*
                scraper.Parallelism = 3;
                scraper.AddToCrawl("http://www.azlyrics.com/");
                scraper.ShouldScrape = (url, prereq) =>
                {
                    return url.Host == "www.azlyrics.com" && url.HasNoQueryParameters();
                };
                */
                scraper.DestinationSuggestedName = "site-outducks.org";
                scraper.AddToCrawl("https://outducks.org/"); 
                using (var s = new CsvReader(@"C:\Users\Andrea\Downloads\isv\inducks_entryurl.isv"))
                {
                    s.Separator = (byte)'^';
                    s.ReadHeader();
                    while (true)
                    {
                        var line = s.ReadLine();
                        if (line == null) break;
                        if (line.Length == 0) continue;
                        var sitecode = line[1].ToStringCached();
                        var path = line[3];
                        if (sitecode == "webusers") sitecode = "webusers/webusers";
                        scraper.AddToCrawl("https://outducks.org/" + sitecode + "/" + path);
                    }
                }
               

               // scraper.ReconsiderSkippedUrls();
                await scraper.ScrapeAsync();
            }
#else
            Console.WriteLine("Bad command line.");
#endif

        }

        private static string Dasherize(string v)
        {
            return Regex.Replace(v, @"[A-Z]", x => "-" + x.Value).ToLowerInvariant().Trim('-');
        }

        private static bool IsMemberTypeOkForDynamicParameter(Type fieldType)
        {
            fieldType = Nullable.GetUnderlyingType(fieldType) ?? fieldType;
            return
                fieldType == typeof(bool) ||
                fieldType == typeof(uint) ||
                fieldType == typeof(int) ||
                fieldType == typeof(ulong) ||
                fieldType == typeof(long) ||
                fieldType == typeof(ushort) ||
                fieldType == typeof(short) ||
                fieldType == typeof(double) ||
                fieldType == typeof(float) ||
                fieldType == typeof(string);
        }

        private static void InitScraperDefaults(WebsiteScraper scraper)
        {
            scraper.CreateThreadProgressDelegate = () => Program.CreateSimpleConsoleProgress("Crawler thread", true);
            scraper.CreateMainProgressDelegate = () => Program.CreateSimpleConsoleProgress("Crawler");
            Console.CancelKeyPress += (s, e) =>
            {
                scraper.Dispose();
            };
            scraper.Parallelism = 10;
            scraper.DatabaseSaveInterval = TimeSpan.FromMinutes(1);
            scraper.OutputAsWarc = true;
            scraper.DestinationBaseDirectory = Configuration_DestinationBaseDirectory;
        }

        [Configuration]
        public static string Configuration_DestinationBaseDirectory = @"C:\WebsiteDumps";

#if NET46
        public static ConsoleProgress<SimpleProgress> CreateSimpleConsoleProgress(string name, bool small = false)
        {
            var progress = ConsoleProgress.Create<SimpleProgress>(name, (p, c) =>
            {
                if (p.Description != null) c.Report(p.Description);
                else c.Report(p.Done, p.Total);
            });
            progress.Controller.BackgroundColor = Console.BackgroundColor;
            progress.Controller.ForegroundColor = ConsoleColor.Magenta;
            progress.Controller.SmallMode = small;
            return progress;
        }
#else
        public static IProgress<SimpleProgress> CreateSimpleConsoleProgress(string name, bool small = false)
        {
            // TODO
            return null;
        }
#endif

    }
}
