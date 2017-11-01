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
using System.Diagnostics;
using System.Text.Utf8;
using System.Globalization;
#if DOM_EMULATION
using Shaman.DomEmulation;
#endif
using System.Threading;

namespace Shaman.Scraping
{
    public class Program
    {
        public static int Main()
        {
            ConfigurationManager.Initialize(typeof(Program).GetTypeInfo().Assembly, IsDebug);
            ConfigurationManager.Initialize(typeof(HttpUtils).GetTypeInfo().Assembly, IsDebug);
            ConfigurationManager.Initialize(typeof(SingleThreadSynchronizationContext).GetTypeInfo().Assembly, IsDebug);

            try
            {
                Shaman.Runtime.SingleThreadSynchronizationContext.Run(MainAsync);
            }
            catch(Exception ex)
            {
                var z = ex.RecursiveEnumeration(x => x.InnerException).Last();
                Console.WriteLine(z);
                return 1;
            }
            finally
            {
                BlobStore.CloseAllForShutdown();
            }
            return 0;
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
        [Configuration(CommandLineAlias = "facebook-update")]
        public static bool Configuration_FacebookUpdate;



        [Configuration(CommandLineAlias = "cookies")]
        public static string Configuration_Cookies;

        [Configuration(CommandLineAlias = "cookie-file")]
        public static string Configuration_CookieFile;

        [Configuration(CommandLineAlias = "site-rules")]
        public static string[] Configuration_SiteRules;

        [Configuration(CommandLineAlias = "site-url")]
        public static string Configuration_SiteUrl;

        [Configuration(CommandLineAlias = "retry-failed")]
        public static bool Configuration_RetryFailed;

        [Configuration(CommandLineAlias = "destination")]
        public static string Configuration_Destination;

        [Configuration(CommandLineAlias = "trim-broken-warcs")]
        public static bool Configuration_TrimBrokenWarcs;

        [Configuration(CommandLineAlias = "reconsider-all")]
        public static bool Configuration_ReconsiderAll;

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
                var name = "--" + Dasherize((item.DeclaringType == typeof(WebsiteScraper) ? "Site" : item.DeclaringType.Name.TrimEnd("Scraper"))) + "-" + Dasherize(item.Name);
                dynamicParameters.Add(name, item);
            }
            //File.WriteAllLines(@"c:\temp\-scraping-extra-parameters.txt", dynamicParameters.Keys.OrderBy(x => x), Encoding.UTF8);

            var positional = Environment.GetCommandLineArgs();

            //Caching.EnableWebCache("/Awdee/Cache/DomEmulation");

            WebsiteScraper commandLineScraper = null;

            if (Configuration_SiteUrl != null)
            {
                commandLineScraper = new WebsiteScraper();
                Program.InitScraperDefaults(commandLineScraper);
                if (Configuration_SiteRules != null)
                    commandLineScraper.Rules = Configuration_SiteRules;
                commandLineScraper.Cookies = cookies;

            }

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
                        else if (mt == typeof(string[])) v = val.SplitFast(',', StringSplitOptions.RemoveEmptyEntries);
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
                    //commandLineScraper.DatabaseSaveInterval = TimeSpan.FromMinutes(10);
                    if (Configuration_Destination != null)
                    {
                        if (Configuration_Destination.Contains("/") || Configuration_Destination.Contains("\\"))
                        {
                            commandLineScraper.DestinationDirectory = Path.GetFullPath(Configuration_Destination);
                        }
                        else
                        {
                            commandLineScraper.DestinationSuggestedName = Configuration_Destination;
                        }
                    }
                    if (Configuration_SiteUrl != null)
                        commandLineScraper.AddToCrawl(Configuration_SiteUrl.AsUri());
                    commandLineScraper.PerformInitialization();
                    if (commandLineScraper is FacebookScraper)
                    {
                        var postsCsv = Path.Combine(commandLineScraper.DestinationDirectory, "Posts.csv");

                        if (
                            !Configuration_RetryFailed &&
                            !Configuration_ReconsiderAll &&
                            !Configuration_FacebookUpdate &&
                            File.Exists(postsCsv)) return;

                        if (Configuration_FacebookUpdate)
                        {
                            var candidates = WarcItem.ReadIndex(Path.Combine(commandLineScraper.DestinationDirectory, "index.cdx"));
                            /*string user = null;
                            first.OpenStream((key, value) => {
                                if (key == "Cookie") user = value.TryCaptureBetween((Utf8String)"c_user=", (Utf8String)";")?.ToString();
                            }).Dispose();*/
                            // the id might also be in the redirected page
                            var user = candidates.Take(2).Select(x => x.ReadText().TryCaptureBetween("USER_ID\":\"", "\"")).FirstOrDefault(x => x != null);
                            string filename =
                                user == "-----" ? "------.txt" :
                                user == null || user == "0" ? (string)null :
                                throw new ArgumentException("Unknown facebook user: " + user);
                            var stopAt = DateTime.ParseExact(File.ReadAllLines(postsCsv)[1].CaptureBefore(","), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                            var fb = (FacebookScraper)commandLineScraper;
                            fb.UpdateUpTo = stopAt;
                            if (File.Exists(Path.Combine(commandLineScraper.DestinationDirectory, "fbgroup-not-a-member"))) return;
                            if (File.Exists(Path.Combine(commandLineScraper.DestinationDirectory, "fberror"))) return;
                            fb.SetStatus(fb.Root, UrlStatus.ToCrawl);

                            if (long.TryParse(fb.Page, out var id))
                            {
                                var url = "https://www.facebook.com/profile.php?id=" + id;
                                if (fb.GetStatus(url) != UrlStatus.UnknownUrl)
                                    fb.SetStatus(url.AsUri(), UrlStatus.ToCrawl);
                            }
                            
                            if (filename != null)
                            {
                                filename = Path.Combine(@"C:\Users\Andrea\OneDrive\QwawaDesktop\Scraping", filename);
                                commandLineScraper.Cookies = File.ReadAllText(filename).Trim().TrimStart("Cookie:").Trim();
                            }

                        }
                    }


                    if (Configuration_ReconsiderAll)
                        commandLineScraper.ReconsiderForScraping("**");
                    else
                        commandLineScraper.ReconsiderSkippedUrls();

                    if (Configuration_RetryFailed)
                        commandLineScraper.ReconsiderFailedUrls();

                    /*
                    foreach (var line in File.ReadAllLines(@"C:\Users\Andrea\Desktop\arcilesbica-robots.txt"))
                    {
                        var l = line.Trim();
                        if (l.Length == 0) continue;
                        commandLineScraper.AddToCrawl("http://www.arcilesbica.it"+l);
                    }
                    */

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
                InitScraperDefaults(scraper);


                /*
                scraper.DestinationSuggestedName = "fb-anisea-friends";
                scraper.Cookies = File.ReadAllText(@"C:\Repositories\Awdee\Awdee2.Declarative\phantomjs\facebookcookiesanisea.txt").Trim();
                scraper.ShouldScrape = (url, prereq) =>
                {
                    if (prereq) return true;
                    return false;
                };
                scraper.ForceLinks = "a:link-has-host-path('m.facebook.com'; '/friends/center/friends/')";
                scraper.AddToCrawl("https://m.facebook.com/friends/center/friends/", true);
                */
                /*
                var friends = scraper.CdxIndex.Values.Where(x => x.Url.Contains("/friends/center/friends/"))
                    .SelectMany(x => 
                    {
                        var zz = x.ReadHtml();
                        return zz.FindAll("div.w.bk").Select(z=> {
                            var m = z.TryGetValue("div.bn.bo", pattern: @"^([,\d]+) mutual");
                            return new FacebookFriend
                            {
                                Name = z.TryGetValue("img", "alt"),
                                MutualFriends = m != null ? int.Parse(m.Replace(",", string.Empty)) : -1,
                                Id = long.Parse(z.GetLinkUrl("a").GetQueryParameter("uid"))
                            };
                        });
                    }).ToList();
                JsonFile.Save(friends, Path.Combine(scraper.DestinationDirectory, "friends.json"));
                */
                /*
                var friends = JsonFile.Read<List<FacebookFriend>>(Path.Combine(scraper.DestinationDirectory, "friends.json"))
                    .OrderByDescending(x => x.MutualFriends);
                    */
                //friends.OpenExcel();
                /*
                await friends.ForEachThrottledAsync(async x =>
                {
                Console.WriteLine(x.Name);
                var p = await scraper.GetHtmlNodeAsync(("https://m.facebook.com/" + x.Id).AsUri());
                }, Debugger.IsAttached ? 1 : scraper.Parallelism);
                */
                /*
                foreach (var item in scraper.CdxIndex.Values.Where(x=>x.Url.EndsWith("_rdr")))
                {
                    Console.WriteLine(item.Url);
                    scraper.CrawlLinks(item.ReadHtml());
                }
                scraper.SaveDatabase();
                await scraper.ScrapeAsync();
                */

                /*
                var actuallyExisting = new HashSet<string>(scraper.CdxIndex.Keys);
                
                foreach (var item in scraper.GetScrapedUrls(false))
                {
                    if (!actuallyExisting.Contains(item.AbsoluteUri))
                    {
                        scraper.SetStatus(item, UrlStatus.ToCrawl);
                    }
                }
                return;*/
                
                var resourcesOnly = true;
                scraper.AddToCrawl("http://knowyourmeme.com/");
                if (!resourcesOnly)
                {
                    scraper.Parallelism = 2;
                    scraper.InterRequestDelay = TimeSpan.FromSeconds(0.4);
                }
                scraper.HtmlReceived += (a, b, page) =>
                {
                    if (page.FindSingle("h3:contains('include your ip address then there')") != null)
                    {
                        scraper.Dispose();
                        throw new Exception("IP banned.");
                    }
                };
                scraper.RewriteLink = (url) =>
                {
                    if (url.Query == "?fb") return url.GetLeftPart(UriPartial.Path).AsUri();
                    return url;
                };
                scraper.ShouldScrape = (url, prereq) =>
                {
                    if (url.Contains("kym") && url.EndsWith(".jpg"))
                    {

                    }
                    if (resourcesOnly && url.IsHostedOn("knowyourmeme.com")) return false;
                    if (!resourcesOnly && !url.IsHostedOn("knowyourmeme.com")) return false;

                    //if (url.IsHostedOn("kym-cdn.com")) return false; // todo
                    if (url.IsHostedOn("imgur.com")) return false; // todo

                    if (url.IsHostedOn("meme.am")) return false;
                    //if (!url.IsHostedOn("kym-cdn.com")) return false;
                    if (prereq) return true;
                    if (!scraper.IsSubfolderOfFirstUrl(url)) return false;
                    if (url.HasQueryParameters()) return false;
                    if (url.AbsolutePath == "/") return true;
                    if (url.PathEndsWith("/photos/page/1")) return false;
                    if (url.PathEndsWith("/videos/page/1")) return false;
                    if (url.PathEndsWith("/editorships")) return false;
                    if (url.PathEndsWith("/ask")) return false;
                    if (url.PathContainsComponent("/photos/trending")) return false;
                    if (url.PathContainsComponent("/videos/trending")) return false;
                    //if (url.PathStartsWith("/page/")) return true;
                    if (url.PathContainsComponent("/memes/popular/")) return false;
                    if (url.PathContainsComponent("/deadpool/")) return false;
                    if (url.PathContainsComponent("/memes/researching/page/")) return false;
                    
                    if (url.PathContainsComponent("/edits/")) return false;
                    if (url.PathContainsComponent("/sort/")) return false;
                    if (url.PathContainsComponent("/favorites/")) return false;
                    if (url.PathContainsComponent("/new/")) return false;
                    if (url.PathStartsWith("/memes/")) return true;
                    
                    //if (url.PathContains("/photos/")) return false;
                    
                    //if (url.PathEndsWith("/children")) return false;
                    return false;
                };

                scraper.ReconsiderForScraping("**");
                
                //await scraper.ScrapeFailedImagesFromKnownHostsAsync();
                
                await scraper.ScrapeAsync();
            }
#else
            Console.WriteLine("Bad command line.");
#endif

        }

        public class FacebookFriend
        {
            public string Name;
            public long Id;
            public int MutualFriends;
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
