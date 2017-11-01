using ProtoBuf;
using Shaman.Collections;
using Shaman.Dom;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Http;
using System.Net;
using System.Runtime.Serialization;
using Shaman.Runtime;
using System.Text.Utf8;
using System.IO.Compression;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;
using Shaman.Runtime.ReflectionExtensions;
using ExCSS;
#if DOM_EMULATION
using Shaman.DomEmulation;
#endif
#if WARC
using CurlSharp;
#endif
#if SHAMAN
using HttpUtils = Shaman.Utils;
using HttpExtensionMethods = Shaman.ExtensionMethods;
#endif

namespace Shaman.Scraping
{
    [RestrictedAccess]
    public partial class WebsiteScraper : IDisposable
    {

        public bool DownloadCssExternalImages = true;
        private bool initializing;
        public void PerformInitialization()
        {
            if (!initialized && !initializing)
            {
                initializing = true;
                try
                {
                    Initialize();
                }
                finally
                { 
                    initializing = false;
                }
                initialized = true;
            }
        }
        private bool initialized;
        protected virtual void Initialize()
        {
        }



        public bool OutputAsWarc;
#if WARC
        public Func<Uri, CurlEasy, MemoryStream, MemoryStream, string> GetDestinationWarc =
        (url, easy, req, res) =>
        {
            if (res.Length > Configuration_WarcBigFilesThreshold) return "large";
            var ct = easy.ContentType?.ToLowerFast();
            if (ct == null || (ct.Contains("html") || ct.Contains("xhtml") || ct.Contains("javascript") || ct.Contains("/css")) || Configuration_NonMediaExtensions.Contains(GetExtension(url))) return "web";
            var ext = GetExtension(url);
            if (ext == ".pdf" || (ct != null && ct.Contains("/pdf"))) return "pdf";
            return "media";
        };

        private static long Configuration_WarcBigFilesThreshold = 10 * 1024 * 1024;



        public void GenerateCdx()
        {
            lock (this)
            {
                CloseWarcs();
                WarcCdxItemRaw.GenerateCdx(DestinationDirectory ?? throw new ArgumentNullException());
                cdxIndex = null;
            }
        }

        protected bool Stop { get; set; }

        internal void AddComplexAjax(Uri initialRequest, string nextPageLinkSelector)
        {
            AddToCrawl(initialRequest);
            if (!addedNextPageLinkSelectorExtractors.Contains(nextPageLinkSelector))
            {
                addedNextPageLinkSelectorExtractors.Add(nextPageLinkSelector);
                CollectAdditionalLinks += (url, node) =>
                {
                    if (Stop) return null;
                    var lazy = new LazyUri(url);
                    NextPageLinkSelection.UpdateNextLink(ref lazy, node, nextPageLinkSelector, alwaysPreserveRemainingParameters: true);
                    if (lazy != null) return new[] { (lazy.Url, false) };
                    return Enumerable.Empty<(Uri, bool)>();
                };
            }
        }

        private List<string> addedNextPageLinkSelectorExtractors = new List<string>();


        public void FlushWarcs()
        {
            lock (this)
            {
                foreach (var open in openWarcs.Values)
                {
                    open.Flush();
                }
            }
        }

        public Dictionary<string, WarcItem> CdxIndex
        {
            get
            {
                lock (this)
                {
                    PerformInitialization();
                    if (cdxIndex != null) return cdxIndex;

                    var path = Path.Combine(DestinationDirectory, "index.cdx");
                    if (!File.Exists(path)) GenerateCdx();
                    var dict = new Dictionary<string, WarcItem>();
                    foreach (var item in WarcItem.ReadIndex(path))
                    {
                        dict[item.Url] = item;
                    }
                    foreach (var warc in openWarcs.Values)
                    {
                        foreach (var item in warc.recordedResponses)
                        {
                            dict[item.Url] = item;
                        }

                    }
                    cdxIndex = dict;
                    return cdxIndex;
                }
            }
        }


#endif




        public IEnumerable<Uri> GetScrapedUrls(bool htmlOnly)
        {
            return Urls.Where(x =>
            {
                var s = GetStatus(x.Value);
                var k = s == UrlStatus.Crawled || s == UrlStatus.DomEmulated;
                if (htmlOnly) return k;
                return k || s == UrlStatus.Downloaded;
            }).Select(x => x.Key.AsUri()).ToList();
        }
        public void ReconsiderSkippedUrls()
        {
            InitDb();
            foreach (var item in Urls.ToList())
            {
                if (WebsiteScraper.GetStatus(item.Value) == UrlStatus.Skipped)
                {
                    ReconsiderForScraping(item.Key.AsUri());
                }
            }
        }

#if WARC
        public HttpClient ArchivingHttpClient { get; private set; }
        public HttpClientHandler ArchivingHttpClientHandler { get; private set; }
        private Dictionary<string, WarcItem> cdxIndex;
#endif


        private SynchronizationContext syncCtx;
        private int threadId;
        private static bool curlInitialized;
        public WebsiteScraper()
        {
#if WARC
            lock (typeof(WebsiteScraper))
            {
                if (!curlInitialized)
                {
                    Curl.GlobalInit(CurlInitFlag.All);
                    curlInitialized = true;
                }
            }
#endif

            syncCtx = SynchronizationContext.Current;
            threadId = Environment.CurrentManagedThreadId;
            if (syncCtx == null) throw new InvalidOperationException("The object must be used within a SynchronizationContext.");

            cts = new CancellationTokenSource();
#if WARC
            var curlHandler = new CurlWarcHandler();
            curlHandler.syncObj = this;

            curlHandler.GetDestinationWarc = (a, b, c, d) =>
            {
                if (!OutputAsWarc) throw new InvalidOperationException("OutputAsWarc must be enabled for ArchivingHttpClient.");
                return GetOrCreateWarcWriter(this.GetDestinationWarc(a, b, c, d));
            };
            curlHandler.OnHtmlRetrieved = (page, url, ex) =>
            {
                Action update = () =>
                {
                    if (ex != null)
                    {
                        int statusCode;
                        var webex = ex.RecursiveEnumeration(x => x.InnerException).OfType<WebException>().FirstOrDefault();
                        if (webex != null && webex.Status != WebExceptionStatus.Success)
                        {
                            statusCode = (int)webex.Status;
                        }
                        else
                        {
                            statusCode = (int)HttpUtils.Error_UnknownError;
                        }
                        SetHttpStatus(url.AbsoluteUri, (HttpStatusCode)statusCode);
                        SetStatus(url.AbsoluteUri, UrlStatus.Error);
                    }
                    if (page != null)
                    {
                        HtmlReceived?.Invoke(url, null, page);
                        CrawlAndFixupLinks(null, page, page.OwnerDocument.PageUrl);
                    }

                };
                if (Environment.CurrentManagedThreadId == threadId) update();
                else syncCtx.Post(update);
            };
            curlHandler.OnResponseReceived = (res, easy, reqms, resms) =>
            {
                Action update = () =>
                {
                    InitDb();

                    var t = IsErrorStatusCode(res.StatusCode) ? UrlStatus.Error : UrlStatus.RetrievedButNotCrawled;
                    LazyUri url = null;
                    if (res.RequestMessage.Properties.TryGetValue("ShamanURL", out var shamanUrlObj))
                        url = (LazyUri)shamanUrlObj;
                    else
                        url = new LazyUri(res.RequestMessage.RequestUri);

                    var ct = easy.ContentType?.ToLowerFast();
                    if (ct != null && ct.Contains("html"))
                    {
                        // already handled by OnHtmlRetrieved 
                    }
                    else if (GetExtension(url.PathConsistentUrl) == ".css" || (ct != null && ct.StartsWith("text/css")))
                    {
                        resms.Position = 0;
                        using (var sr = new StreamReader(resms, WebSiteEncoding ?? Encoding.UTF8))
                        {
                            CrawlCss(sr.ReadToEnd(), url.Url);
                        }
                        resms.Position = 0;
                        SetStatus(url.Url, UrlStatus.Crawled);
                    }
                    else
                    {
                        resms.Position = 0;
                        NonHtmlReceived?.Invoke(url.Url, easy, resms);
                        resms.Position = 0;
                    }

                    SetStatus(url.Url, t);
                    SetHttpStatus(url.AbsoluteUri, res.StatusCode);
                };
                if (Environment.CurrentManagedThreadId == threadId) update();
                else syncCtx.Post(update);
            };
            curlHandler.TryGetCached = request =>
            {
                request.Properties.TryGetValue("ShamanURL", out var shamanUrlObj);
                var shamanUrl = shamanUrlObj as LazyUri;
                lock (this)
                {
                    if (CdxIndex.TryGetValue(shamanUrl?.AbsoluteUri ?? request.RequestUri.AbsoluteUri, out var warcItem))
                    {
                        if (warcItem.ResponseCode != 0)
                        {
                            var open = openWarcs.Values.FirstOrDefault(x => x.WarcName == warcItem.WarcFile);
                            if (open != null)
                            {
                                lock (open)
                                {
                                    open.Flush();
                                }
                            }
                            var response = new HttpResponseMessage(warcItem.ResponseCode);
                            var stream = warcItem.OpenStream((name, value) =>
                            {
                                response.Headers.TryAddWithoutValidation(name.ToStringCached(), value.ToString());
                            });
                            response.Content = new StreamContent(stream);
                            return response;
                        }
                    }
                }
                return null;
            };
            this.ArchivingHttpClientHandler = curlHandler;
            this.ArchivingHttpClient = new HttpClient(curlHandler);
#endif
        }

        private HtmlNode CreateShamanPage(string url, HttpResponseMessage resms)
        {
            var lazy = new LazyUri(url);
            var messageBox = new HttpExtensionMethods.HttpRequestMessageBox();
            messageBox.PrebuiltResponse = resms;
            messageBox.PrebuiltRequest = resms.RequestMessage;
            var options = new WebRequestOptions();
            var metaParameters = HttpExtensionMethods.ProcessMetaParameters(lazy, options) ?? new Dictionary<string, string>();

            var info = HttpExtensionMethods.SendAsync(lazy, options, messageBox, alwaysCatchAndForbidRedirects: true, keepResponseAliveOnError: true, synchronous: true).AssumeCompleted();
          
            if (info.Exception != null)
            {
                HttpExtensionMethods.CheckErrorSelectorAsync(lazy, info, options, metaParameters, synchronous: true).AssumeCompleted();
                throw info.Exception.Rethrow();
            }


            var html = HttpExtensionMethods.ParseHtmlAsync(info, null, null, options, metaParameters, lazy, synchronous: true).AssumeCompleted();

            return html;
        }

        private TaskCompletionSource<bool> disposed = new TaskCompletionSource<bool>();

#if WARC && DOM_EMULATION
        public void SetDomEmulated(string url)
        {
            var oldstatus = GetStatus(url);
            if (oldstatus != UrlStatus.Crawled && oldstatus != UrlStatus.DomEmulated) throw new Exception("Pages must be crawled before they can be DOM emulated");
            SetStatus(url, UrlStatus.DomEmulated);
        }

        private List<Uri> ignorableScripts;

        public async Task<IReadOnlyList<Uri>> GetIgnorableScriptsAsync(DomEmulator example, Func<RequestInfo, bool> isTargetRequest, Func<DomEmulator, Task> interact = null)
        {
            if (ignorableScripts != null) return ignorableScripts;
            var dir = DestinationDirectory;
            var cacheFile = dir != null ? Path.Combine(dir, ".ignorable-scripts") : null;
            if (File.Exists(cacheFile))
            {
                ignorableScripts = File.ReadAllLines(cacheFile).Select(x => x.Trim()).Where(x => !string.IsNullOrEmpty(x) && !x.StartsWith("#")).Select(x => new Uri(x)).ToList();
                return ignorableScripts;
            }
            ignorableScripts = await DomEmulator.LearnIgnorableScriptsAsync(example, isTargetRequest, interact);
            if (cacheFile != null)
            {
                Directory.CreateDirectory(dir);
                File.WriteAllLines(cacheFile, ignorableScripts.Select(x => x.AbsoluteUri));
            }

            return ignorableScripts;
        }


        // TODO
        public async Task ScrapeFailedImagesFromKnownHostsAsync()
        {
            foreach (var item in GetMatchingUrls("**fbcdn.net**"))
            {
                var photo = Shaman.Connectors.Facebook.Photo.TryGetPhoto(item);
                if (photo != null)
                {
                    try
                    {
                        await photo.LoadDetailsAsync(this.ArchivingHttpClient);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                    if (photo.LargestImage != null)
                    {
                        Console.WriteLine(photo.LargestImage.Url);
                        AddToCrawl(photo.LargestImage.Url, true);
                    }
                }
            }
        }
#endif
        public void ReconsiderForScraping(Uri item)
        {
            InitDb();
            var s = GetStatus(item);
            if (s == UrlStatus.Error || s == UrlStatus.Skipped) SetStatus(item, UrlStatus.ToCrawl);
            else AddToCrawl(item);
        }
        public void ReconsiderForScraping(string model)
        {
            InitDb();
            foreach (var item in GetMatchingUrls(model))
            {
                ReconsiderForScraping(item);
            }
        }



        public static Func<Uri, bool> LoadExclusions(params string[] files)
        {
            return ParseExclusions(string.Join("\n", files.Select(x => File.ReadAllText(x, Encoding.UTF8))));
        }

        protected IDictionary<string, string> SerializedProperties
        {
            get
            {
                InitDb();
                if (db.Content.Properties == null)
                    db.Content.Properties = new Dictionary<string, string>();
                return db.Content.Properties;
            }
        }


        public void SaveDatabase()
        {
            CheckDisposed();
            if (db == null) return;
            db.Save();
            isDbDirty = false;
        }

        private static Func<Uri, bool> ParseExclusions(string exclusions)
        {
            var sr = new StringReader(exclusions);
            var regex = new StringBuilder();
            while (true)
            {
                var line = sr.ReadLine();
                if (line == null) break;
                line = line.Trim();
                if (line.StartsWith("#") || string.IsNullOrEmpty(line)) continue;
                if (regex.Length != 0) regex.Append("|");
                regex.Append("(?:");
                regex.Append(line);
                regex.Append(")");
            }
            var r = new Regex(regex.ToString());
            return url =>
            {
                return r.IsMatch(url.AbsoluteUri);
            };
        }

        public string DestinationDirectory
        {
            get
            {
                if (_destinationDirectory != null)
                    return _destinationDirectory;
                if ((DestinationSuggestedName == null && _suggestedNameFallback == null) || DestinationBaseDirectory == null)
                    return null;
                return Path.Combine(DestinationBaseDirectory, DestinationSuggestedName ?? _suggestedNameFallback);
            }
            set
            {
                _destinationDirectory = value;
            }
        }
        private string _destinationDirectory;
        public string DestinationSuggestedName { get; set; }
        public string DestinationBaseDirectory { get; set; }

        public int Parallelism { get; set; } = 5;

        public string ProgressFile { get; set; }

        public Func<Uri, Uri> RewriteLink { get; set; }
        public Func<IProgress<SimpleProgress>> CreateThreadProgressDelegate { get; set; }
        public Func<IProgress<SimpleProgress>> CreateMainProgressDelegate { get; set; }


        public Func<string, int> UrlPriorityDelegate { get; set; }

        private JsonFile<CrawlerStatus> db;
        private CancellationTokenSource cts;
        private List<FileStream> currentFiles = new List<FileStream>();

        public static Func<Uri, Uri> CreateHttpsWwwNormalizer(Uri root, bool addTrailingSlash)
        {

            var baseRootNoWww = root.Host.TrimStart("www.");
            var baseRootWww = "www." + baseRootNoWww;
            var baseRootUsesWww = root.Host.StartsWith("www.");

            return url =>
            {
                if (!HttpUtils.IsHttp(url)) return null;
                var shouldAddSlash = addTrailingSlash && !url.AbsolutePath.EndsWith("/") && GetExtension(url) == null;
                var slash = shouldAddSlash ? "/" : null;
                if (url.Host == baseRootNoWww && baseRootUsesWww)
                {
                    return (root.Scheme + "://" + baseRootWww + url.AbsolutePath + slash + url.Query + url.Fragment).AsUri();
                }
                else if (url.Host == baseRootWww && !baseRootUsesWww)
                {
                    return (root.Scheme + "://" + baseRootNoWww + url.AbsolutePath + slash + url.Query + url.Fragment).AsUri();
                }
                else if ((url.Host == baseRootWww || url.Host == baseRootNoWww) && url.Scheme != root.Scheme)
                {
                    return (root.Scheme + "://" + url.Authority + url.AbsolutePath + slash + url.Query + url.Fragment).AsUri();
                }
                else if ((url.Host == baseRootNoWww || url.Host == baseRootWww) && shouldAddSlash)
                {
                    return (url.Scheme + "://" + url.Authority + url.AbsolutePath + slash + url.Query + url.Fragment).AsUri();
                }
                return null;
            };
        }

        public bool IsSubfolderOfFirstUrl(string url, bool ignoreLastComponentOfInitialUrl = false)
        {
            return IsSubfolderOfFirstUrl(url.AsUri(), ignoreLastComponentOfInitialUrl);
        }

        public bool IsSubfolderOfFirstUrl(Uri url, bool ignoreLastComponentOfInitialUrl = false)
        {
            return UrlRuleMatcher.IsSubfolderOf(url, _firstAddedUrl, ignoreLastComponentOfInitialUrl);
        }



      

        public void ReconsiderFailedUrls()
        {
            foreach (var item in Urls.ToList())
            {
                if (WebsiteScraper.GetStatus(item.Value) == UrlStatus.Error)
                {
                    ReconsiderForScraping(item.Key.AsUri());
                }
            }
        }



        public bool IsPrerequisite(Uri url)
        {
            return IsPrerequisite(url.AbsoluteUri);
        }
        public bool IsPrerequisite(string url)
        {
            InitDb();
            db.Content.Urls2.TryGetValue(url, out var k);
            return (k & MaskIsPrerequisite) != 0;
        }
        private bool initializingDb;
        private void InitDb()
        {
            CheckDisposed();
            if (db != null) return;
            if (initializingDb) throw new InvalidOperationException();
            initializingDb = true;
            PerformInitialization();
            if (DestinationDirectory == null) throw new ArgumentException();
            if (ProgressFile == null)
            {
                ProgressFile = Path.Combine(DestinationDirectory, "Progress.pb");
            }
            Directory.CreateDirectory(DestinationDirectory);
            lockFile = File.Open(Path.Combine(DestinationDirectory, ".lock"), FileMode.Create, FileAccess.Write, FileShare.None);
            db = JsonFile.Open<CrawlerStatus>(ProgressFile);
            db.MaximumUncommittedTime = DatabaseSaveInterval;
            if (db.Content.Urls2 == null)
            {
                db.Content.Urls2 = new Dictionary<string, ushort>();
                if (Directory.EnumerateFiles(DestinationDirectory, "*.warc.gz").Any())
                    shouldPopulateProgressDatabase = true;
            }
            //db.Content.Urls2 = db.Content.Urls.ToDictionary(x => x.Key, x => (ushort)((int)x.Value << 11));

            foreach (var item in db.Content.Urls2.ToList())
            {

                if (GetStatus(item.Value) == UrlStatus.Processing) SetStatus(item.Key, UrlStatus.ToCrawl);
            }
            initializingDb = false;
            DatabaseInitialized?.Invoke();

#if WARC
            if (shouldPopulateProgressDatabase) PopulateProgressDatabase();
#endif
        }

        private bool shouldPopulateProgressDatabase;
#if WARC
        private void PopulateProgressDatabase()
        {
            PerformInitialization();
            InitDefaultDelegates();
            var warcitems = CdxIndex.Values;
            var sp = new Scratchpad();
            foreach (var item in warcitems)
            {
                sp.Reset();
                Console.WriteLine(item.Url);
                if (IsErrorStatusCode(item.ResponseCode))
                {
                    SetStatus(item.Url, UrlStatus.Error);
                    SetHttpStatus(item.Url, item.ResponseCode);
                }
                else
                {
                    SetHttpStatus(item.Url, item.ResponseCode);
                    using (var reader = new Utf8StreamReader(item.OpenRaw()))
                    {
                        reader.ReadTo((Utf8String)"\r\n\r\n");
                        HandleHttpResponse(item.Url, sp, item.Url.AsUri(), reader, null, -1);
                    }

                }
            }
            SaveDatabase();
        }
#endif
        public IEnumerable<Uri> GetMatchingUrls(string model)
        {
            InitDb();
            return db.Content.Urls2.Keys.Where(x => x.IsMatchSimple(model)).Select(x => x.AsUri()).ToList();
        }

        public IEnumerable<Uri> GetFailedUrls(string model)
        {
            InitDb();
            return db.Content.Urls2.Where(x => GetStatus(x.Value) == UrlStatus.Error && x.Key.IsMatchSimple(model)).Select(x => x.Key.AsUri()).ToList();
        }

        public void SetStatus(Uri key, UrlStatus status)
        {
            SetStatus(key.AbsoluteUri, status);
        }

        private bool isDbDirty = false;

        private void SetStatus(string key, UrlStatus status)
        {
            isDbDirty = true;
            db.Content.Urls2.TryGetValue(key, out var p);

            if (status == UrlStatus.Crawled && GetStatus(p) == UrlStatus.DomEmulated)
                status = UrlStatus.DomEmulated;

            p &= unchecked((ushort)~MaskKind);
            p |= unchecked((ushort)((int)status << 11));
            db.Content.Urls2[key] = p;

        }
        private void SetHttpStatus(string key, HttpStatusCode httpStatus)
        {
            isDbDirty = true;
            db.Content.Urls2.TryGetValue(key, out var p);

            p &= unchecked((ushort)~MaskHttpStatus);
            p |= unchecked((ushort)((int)httpStatus));
            db.Content.Urls2[key] = p;
        }

        public int GetHttpStatus(ushort value)
        {
            return value & MaskHttpStatus;
        }

        public static UrlStatus GetStatus(ushort value)
        {
            return (UrlStatus)((value & MaskKind) >> 11);
        }

        public void SetToTryAgain(string key)
        {
            SetStatus(key, UrlStatus.ToCrawl);
        }

        public UrlStatus GetStatus(string url)
        {
            InitDb();
            if (db.Content.Urls2.TryGetValue(url, out var s))
            {
                return GetStatus(s);
            }
            return UrlStatus.UnknownUrl;
        }
        public UrlStatus GetStatus(Uri url)
        {
            return GetStatus(url.AbsoluteUri);
        }



        public IEnumerable<KeyValuePair<string, ushort>> Urls
        {
            get
            {
                InitDb();
                return db.Content.Urls2;
            }
        }

        public static Encoding EncodingWindows1252 => Encoding.GetEncoding("windows-1252");

        public static string[] Configuration_NonMediaExtensions = new[] { ".html", ".htm", ".js", ".css", ".json" };

        public string ForceLinks { get; set;}

        public void RewriteLinks(Uri uri)
        {
            InitDb();
            var path = GetPath(uri);
            if (path == null) return;
            var page = TryGetSavedPage(uri, path);
            if (page != null)
            {
                CrawlAndFixupLinks(path, page, uri);
            }
        }
        private IProgress<SimpleProgress> mainprogress;
        private int processed;

        public void SetMainProgressStatus(string description)
        {
            mainprogress.Report(description);
        }

        private const ushort MaskHttpStatus =
            0b0000_0011_1111_1111;
        private const ushort MaskIsPrerequisite =
            0b0000_0100_0000_0000;
        private const ushort MaskKind =
            0b0111_1000_0000_0000;


        private HashSet<string> tocrawl;
        private DelegateHeap<string> tocrawlHeap;
        private int runningDownloads;




#if WARC
        private void CloseWarcs()
        {
            lock (this)
            {
                foreach (var item in pooledEasyHandles)
                {
                    item.Dispose();
                }
                pooledEasyHandles.Clear();
                foreach (var item in pooledRequestMemoryStreams)
                {
                    item.Dispose();
                }
                pooledRequestMemoryStreams.Clear();
                foreach (var item in pooledResponseMemoryStreams)
                {
                    item.Dispose();
                }
                pooledResponseMemoryStreams.Clear();
                AppendNewItemsToIndex();
                foreach (var item in openWarcs)
                {
                    lock (item.Value)
                    {
                        item.Value.Dispose();
                    }
                }
                openWarcs.Clear();
            }
        }
#endif
        private bool hasAddedUrlsBeforeShouldScrapeAvailable;
        public async Task ScrapeAsync()
        {
            CheckDisposed();
            PerformInitialization();
            var folder = DestinationDirectory;
            if (folder == null) throw new ArgumentException("Neither DestinationDirectory nor DestinationDirectoryBase have been specified.");
            InitDefaultDelegates();
#if !WARC
            if (OutputAsWarc) throw new NotSupportedException();

#endif

            var ct = cts.Token;
            InitDb();

            if (hasAddedUrlsBeforeShouldScrapeAvailable)
            {
                ReconsiderSkippedUrls();
                hasAddedUrlsBeforeShouldScrapeAvailable = false;
            }
            var fatal = new TaskCompletionSource<bool>();
            {
                mainprogress = CreateMainProgressDelegate != null ? CreateMainProgressDelegate() : null;
                mainprogress?.Report(new SimpleProgress(0, 1));


                tocrawl = new HashSet<string>(db.Content.Urls2.Where(x => GetStatus(x.Value).In(UrlStatus.ToCrawl, UrlStatus.Skipped)).Select(x => x.Key));
                tocrawlHeap = new Shaman.Collections.DelegateHeap<string>(UrlPriorityDelegate ?? (x => x.Length), tocrawl);

                runningDownloads = 0;
                var tasks = new List<Task>();
                processed = db.Content.Urls2.Count(x => GetStatus(x.Value).In(UrlStatus.Crawled, UrlStatus.Downloaded, UrlStatus.DomEmulated));
                for (int i = 0; i < (Debugger.IsAttached ? 1 : Parallelism); i++)
                {
                    tasks.Add(CreateTask(async () =>
                    {
                        try
                        {
#if WARC
                            var scratchpad = new Scratchpad();
#endif
                            var progress = CreateThreadProgressDelegate != null ? CreateThreadProgressDelegate() : null;

                            while (true)
                            {
                                while (tocrawl.Count == 0)
                                {
                                    if (ct.IsCancellationRequested) return;
                                    progress.Report("Waiting for URLs");
                                    await Task.Delay(1000);
                                    if (runningDownloads == 0)
                                    {
                                        progress.Report("Nothing else to do.");
                                        return;
                                    }
                                }
                                if (ct.IsCancellationRequested) return;

                                runningDownloads++;
                                var pageUrl = tocrawlHeap.ExtractDominating();
                                tocrawl.Remove(pageUrl);
#if WARC
                                if (OutputAsWarc)
                                {
                                    scratchpad.Reset();
                                    await ProcessPageWarcAsync(pageUrl, scratchpad, progress);
                                }
                                else
#endif
                                {
                                    await ProcessPageFileSystemAsync(pageUrl, progress);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            fatal.TrySetException(ex);
                        }
                    }));
                }


                await Task.WhenAny(Task.WhenAll(tasks), fatal.Task);
                if (fatal.Task.IsFaulted) fatal.Task.Exception.InnerException.Rethrow();
                fatal.TrySetResult(true);
            }


        }

        private void InitDefaultDelegates()
        {

            if (ForceLinks != null)
            {
                CollectAdditionalLinks += (url, page) =>
                {
                    return page.FindAll(ForceLinks).Select(x => x.TryGetLinkUrl()).Where(x => x != null).Select(x => (x, true));
                };
            }

            if (ShouldScrape == null)
            {
                if (RewriteLink == null)
                {
                    var normalizer = CreateHttpsWwwNormalizer(FirstAddedUrl, false);
                    RewriteLink = x =>
                    {
                        return normalizer(x);
                    };
                }

                ShouldScrape = UrlRuleMatcher.GetMatcher(Rules, _firstAddedUrl, false);
                //throw new ArgumentException("ShouldScrape was not configured.");
            }
        }

        public void AddToCrawl(IEnumerable<Uri> urls)
        {
            foreach (var item in urls)
            {
                AddToCrawl(item);
            }
        }


        private string _suggestedNameFallback;
        private Uri _firstAddedUrl;
        public Uri FirstAddedUrl => _firstAddedUrl;
        public event Action DatabaseInitialized;
        public void AddToCrawl(string url, bool isPrerequisite = false)
        {
            CheckDisposed();
            if (url.StartsWith("data:")) return;
            if (_firstAddedUrl == null)
            {
                _firstAddedUrl = url.AsUri();
                if (_destinationDirectory == null && DestinationSuggestedName == null && _suggestedNameFallback == null)
                {
                    _suggestedNameFallback = "site-" + _firstAddedUrl.Host.TrimStart("www.");
                }
            }

            if (initializingDb)
            {
                DatabaseInitialized += () => AddToCrawl(url, isPrerequisite);
                return;
            }
            InitDb();
            if (db.Content.Urls2.TryGetValue(url, out var p))
            {
                var original = p;

                var s = GetStatus(p);
                if (isPrerequisite) p |= MaskIsPrerequisite;

                if (ShouldScrape == null)
                {
                    hasAddedUrlsBeforeShouldScrapeAvailable = true;
                }
                else if (s == UrlStatus.Skipped)
                {
                    if (ShouldScrape(url.AsUri(), IsPrerequisite(p)) != false)
                    {
                        p &= unchecked((ushort)~MaskKind);
                        p |= unchecked((ushort)((int)UrlStatus.ToCrawl << 11));
                        if (tocrawl != null)
                        {
                            tocrawl.Add(url);
                            tocrawlHeap.Add(url);
                        }
                    }
                }

                if (p != original)
                {
                    db.Content.Urls2[url] = p;
                }
            }
            else
            {
                if (isPrerequisite) p |= MaskIsPrerequisite;
                var scrape = ShouldScrape != null ? ShouldScrape(url.AsUri(), isPrerequisite) != false : false;
                if (ShouldScrape == null) hasAddedUrlsBeforeShouldScrapeAvailable = true;
                p &= unchecked((ushort)~MaskKind);
                p |= unchecked((ushort)((int)(scrape ? UrlStatus.ToCrawl : UrlStatus.Skipped) << 11));
                if (scrape && tocrawl != null)
                {
                    tocrawl.Add(url);
                    tocrawlHeap.Add(url);
                }

                db.Content.Urls2[url] = p;

            }

        }

        private static bool IsPrerequisite(ushort p)
        {
            return (p & MaskIsPrerequisite) != 0;
        }

        public void AddToCrawl(Uri url)
        {
            AddToCrawl(url, false);
        }

        public void AddToCrawl(Uri url, bool isprerequisite)
        {
            AddToCrawl(url.AbsoluteUri, isprerequisite);
        }

#if WARC

        public Action<Uri, CurlEasy, HttpStatusCode> OnError;

        [Configuration]
        private static long Configuration_MaxWarcLength = 512 * 1024 * 1024;

        private List<MemoryStream> pooledRequestMemoryStreams = new List<MemoryStream>();
        private List<MemoryStream> pooledResponseMemoryStreams = new List<MemoryStream>();
        private List<CurlEasy> pooledEasyHandles = new List<CurlEasy>();


        public event Action<Uri, CurlEasy, Stream> NonHtmlReceived;
        public event Action<Uri, CurlEasy, HtmlNode> HtmlReceived;

        private async Task ProcessPageWarcAsync(string pageUrl, Scratchpad scratchpad, IProgress<SimpleProgress> progress)
        {
            Sanity.Assert(Environment.CurrentManagedThreadId == threadId);

            SetStatus(pageUrl, UrlStatus.Processing);

            bool hasAwaited = false;

            processed++;
            var pageUrlUrl = pageUrl.AsUri();
            var prereq = IsPrerequisite(db.Content.Urls2[pageUrl]);
            if (ShouldScrape(pageUrlUrl, prereq) == false)
            {
                SetStatus(pageUrl, UrlStatus.Skipped);
            }
            else
            {
                var rewritten = Normalize(pageUrlUrl);
                if (!HttpUtils.UrisEqual(pageUrlUrl, rewritten))
                {
                    SetStatus(pageUrl, UrlStatus.Skipped);
                    AddToCrawl(rewritten, prereq);
                }
                else
                {


                    progress.Report(pageUrl.ToString());


                    if (pageUrl.Contains("#$"))
                    {
                        await PerformInterRequestDelayAsync();
                        
                        var response = await pageUrl.AsUri().GetHtmlNodeAsync(MakeNewWebRequestOptions());
                    }
                    else
                    {


                        var requestMs = BorrowPooled(pooledRequestMemoryStreams);
                        var responseMs = BorrowPooled(pooledResponseMemoryStreams);
                        var easy = BorrowPooled(pooledEasyHandles);
                        hasAwaited = true;
                        easy.Cookie = Cookies;
                        easy.ConnectTimeout = 30000;
                        await PerformInterRequestDelayAsync();

                        var (errorStatusCode, curlCode, warcItem) = await ScrapeAsync(easy, null, pageUrl, requestMs, responseMs, es =>
                        {
                            if (IsErrorStatusCode((HttpStatusCode)es.ResponseCode)) return null;
                            var name = GetDestinationWarc(pageUrlUrl, es, requestMs, responseMs);
                            return GetOrCreateWarcWriter(name);
                        }, this, cts.Token).WithCancellation(cts.Token);

                        if (IsErrorStatusCode(errorStatusCode) || curlCode != CurlCode.Ok)
                        {
                            progress.Report("ERROR: " + pageUrl.ToString());
                            SetStatus(pageUrl, UrlStatus.Error);
                            if (curlCode != CurlCode.Ok) errorStatusCode = (HttpStatusCode)(800 + (int)curlCode);
                            SetHttpStatus(pageUrl, errorStatusCode);
                            Console.WriteLine("Error:" + pageUrl.ToString() + " [" + HttpStatusOrErrorCodeToString(errorStatusCode) + "]");
                            OnError?.Invoke(pageUrlUrl, easy, errorStatusCode);
                            await Task.Delay(500);
                        }
                        else
                        {
                            responseMs.Position = 0;

                            HandleHttpResponse(pageUrl, scratchpad, pageUrlUrl, responseMs, easy, responseMs.Length);
                        }


                        ReleasePooled(pooledRequestMemoryStreams, requestMs);
                        ReleasePooled(pooledResponseMemoryStreams, responseMs);
                        ReleasePooled(pooledEasyHandles, easy);
                    }
                }
            }
            runningDownloads--;
            if (hasAwaited)
            {
                mainprogress?.Report(new SimpleProgress(processed, tocrawl.Count + processed));
                db.IncrementChangeCountAndMaybeSave();
            }
        }

        private void HandleHttpResponse(string pageUrl, Scratchpad scratchpad, Uri pageUrlUrl, Stream responseMs, CurlEasy easy, long httpResponseLength)
        {
            using (var stream = WarcItem.OpenHttp(new Utf8StreamReader(responseMs), scratchpad, pageUrlUrl, httpResponseLength, out var payloadLength, out var redirectLocation, out var responseCode, out var contentType, out var lastModified, null))
            {


                if (redirectLocation != null)
                {
                    AddToCrawl(redirectLocation);
                    SetStatus(pageUrl, UrlStatus.Redirect);
                }
                else
                {
                    var ct = contentType.Length == 0 ? null : contentType.ToStringCached().ToLowerFast();

                    if (ct != null && ct.Contains("html"))
                    {
                        var doc = new HtmlDocument();
                        doc.Load(stream, WebSiteEncoding);
                        doc.SetPageUrl(pageUrlUrl);
                        HtmlReceived?.Invoke(pageUrlUrl, easy, doc.DocumentNode);
                        CrawlAndFixupLinks(null, doc.DocumentNode, pageUrlUrl);
                        SetStatus(pageUrl, UrlStatus.Crawled);
                    }
                    else if (GetExtension(pageUrlUrl) == ".css" || (ct != null && ct.StartsWith("text/css")))
                    {
                        using (var sr = new StreamReader(stream, WebSiteEncoding ?? Encoding.UTF8))
                        {
                            CrawlCss(sr.ReadToEnd(), pageUrlUrl);
                        }

                        SetStatus(pageUrl, UrlStatus.Crawled);
                    }
                    else
                    {
                        NonHtmlReceived?.Invoke(pageUrlUrl, easy, stream);
                        SetStatus(pageUrl, UrlStatus.Downloaded);
                    }
                }
            }
        }

        private WarcWriter GetOrCreateWarcWriter(string name)
        {
            if (name == null) return null;
            lock (this)
            {
                var destdir = DestinationDirectory;
                if (destdir == null) throw new ArgumentNullException("DestinationDirectory");
                openWarcs.TryGetValue(name, out var writer);
                if (writer == null || writer.Length > Configuration_MaxWarcLength)
                {
                    if (writer != null)
                    {
                        AppendNewItemsToIndex(writer);
                        writer.Dispose();
                    }
                    var baseName = "archive-" + name + "-";
                    string dest;
                    int n = writer != null ? writer.num + 1 : 0;
                    Directory.CreateDirectory(destdir);
                    do
                    {
                        dest = Path.Combine(destdir, baseName + n.ToString("00000") + ".warc.gz");
                        n++;
                    } while (File.Exists(dest));
                    writer = new WarcWriter(File.Open(dest, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read));
                    writer.WarcName = Path.GetFileName(dest);
                    writer.FullName = dest;
                    writer.onNewWarcItem = x =>
                    {
                        lock (this)
                        {
                            if (cdxIndex != null)
                                cdxIndex[x.Url] = x;
                        }
                    };
                    writer.WriteWarcInfo();
                    writer.num = n - 1;
                    openWarcs[name] = writer;
                }

                return writer;
            }
        }

        private static Utf8String GetHeaderValue(Utf8String line)
        {
            var idx = line.IndexOf((byte)':');
            if (idx == -1) throw new InvalidDataException();
            return line.Substring(idx + 1).Trim();
        }

        private T BorrowPooled<T>(List<T> pool) where T : new()
        {
            if (pool.Count == 0) return new T();
            var m = pool.Last();
            pool.RemoveAt(pool.Count - 1);
            return m;
        }

        private void ReleasePooled<T>(List<T> pool, T ms)
        {
            pool.Add(ms);
        }


#endif


        private static string HttpStatusOrErrorCodeToString(HttpStatusCode errorStatusCode)
        {
#if WARC
            if ((int)errorStatusCode >= 800)
            {
                return ((CurlCode)((int)errorStatusCode - 800)).ToString();
            }
#endif
            return HttpUtils.HttpStatusOrErrorCodeToString(errorStatusCode);
        }

        private static bool IsErrorStatusCode(HttpStatusCode responseCode)
        {
            return (int)responseCode < 100 || (int)responseCode >= 400;
        }


        private async Task ProcessPageFileSystemAsync(string pageUrl, IProgress<SimpleProgress> progress)
        {
            SetStatus(pageUrl, UrlStatus.Processing);

            bool hasAwaited = false;

            processed++;
            var pageUrlUrl = pageUrl.AsUri();
            var rewritten = Normalize(pageUrlUrl);
            string path;
            if (!HttpUtils.UrisEqual(pageUrlUrl, rewritten))
            {
                path = null;
                if (!db.Content.Urls2.ContainsKey(rewritten.AbsoluteUri))
                {
                    AddToCrawl(rewritten);
                }
            }
            else
            {
                path = GetPath(pageUrlUrl);
            }


            if (path != null)
            {

                var page = TryGetSavedPage(pageUrlUrl, path);

                if (page == null && File.Exists(path))
                {
                    SetStatus(pageUrl, UrlStatus.Downloaded);
                }
                else if (page == null)
                {
                    progress.Report(pageUrl.ToString());
                    HttpStatusCode errorStatusCode = 0;
                    try
                    {
                        hasAwaited = true;
                        var now = DateTime.UtcNow;
                        var options = new WebRequestOptions();
                        options.Cookies = Cookies;
                        await PerformInterRequestDelayAsync();
                        using (var response = await pageUrlUrl.GetAsync(options))
                        {

                            if (!response.IsSuccessStatusCode)
                                errorStatusCode = response.StatusCode;
                            response.EnsureSuccessStatusCode();
                            var lm = response.Content.Headers.LastModified?.UtcDateTime;
                            var finalurl = Normalize(response.RequestMessage.RequestUri);
                            var skipped = false;
                            if (!HttpUtils.UrisEqual(finalurl, pageUrlUrl))
                            {
                                var finalurlabs = finalurl.AbsoluteUri;
                                SetStatus(pageUrl, UrlStatus.Redirect);
                                AddToCrawl(finalurl);
                                skipped = true;
                                response.Dispose();
                            }

                            if (!skipped)
                            {
                                var contentType = response.Content.Headers.ContentType?.MediaType;
                                var contentLength = response.Content.Headers.ContentLength;
                                if (contentLength != null && MaxSize != null && contentLength > MaxSize)
                                    throw new WebException("Too big.", HttpUtils.Error_SizeOutsideAcceptableRange);
                                using (var stream = await response.Content.ReadAsStreamAsync())
                                {
                                    if (contentType != null && (contentType.Contains("/html") || contentType.Contains("/xhtml")))
                                    {
                                        var doc = new HtmlDocument();
                                        doc.Load(stream, WebSiteEncoding ?? Encoding.UTF8);
                                        doc.SetPageUrl(finalurl);

                                        var head = doc.DocumentNode.FindSingle("head,html") ?? doc.DocumentNode;
                                        AddMeta(head, "shaman-date-retrieved", now.ToString("o"));
                                        AddMeta(head, "shaman-url", finalurl.AbsoluteUri);
                                        if (lm != null)
                                            AddMeta(head, "shaman-original-date", lm.Value.ToString("o"));

                                        page = doc.DocumentNode;
                                    }
                                    else
                                    {


                                        Directory.CreateDirectory(Path.GetDirectoryName(path));
                                        try
                                        {
                                            var filestream = File.Open(path, FileMode.Create, FileAccess.Write, FileShare.Delete);

                                            try
                                            {
                                                progress.Report(new SimpleProgress(0));
                                                lock (currentFiles) { currentFiles.Add(filestream); }
                                                using (filestream)
                                                using (var progressStream = new StreamWithProgressCallback(stream, p =>
                                                {
                                                    if (MaxSize != null && p > MaxSize) throw new Exception("Too big.");
                                                    if (contentLength != null)
                                                        progress.Report(new SimpleProgress((double)p / contentLength.Value));

                                                }))
                                                {
                                                    await progressStream.CopyToAsync(filestream);
                                                    if (contentLength != null && filestream.Length != contentLength)
                                                        throw new Exception("Incomplete response.");
                                                }

                                                if (lm != null)
                                                    File.SetLastWriteTimeUtc(path, lm.Value);
                                                progress.Report(new SimpleProgress(1));
                                            }
                                            finally
                                            {
                                                lock (currentFiles) { currentFiles.Remove(filestream); }
                                            }
                                        }
                                        catch
                                        {
                                            File.Delete(path);
                                            throw;
                                        }

                                        SetStatus(pageUrl, UrlStatus.Downloaded);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {

                        progress.Report("ERROR: " + pageUrl.ToString());
                        SetStatus(pageUrl, UrlStatus.Error);
                        if (ex.RecursiveEnumeration(x => x.InnerException).Any(x => x is TimeoutException)) errorStatusCode = (HttpStatusCode)WebExceptionStatus.Timeout;
                        else
                        {
                            var webex = (WebException)ex.RecursiveEnumeration(x => x.InnerException).FirstOrDefault(x => x is WebException);
                            if (webex != null)
                            {
                                errorStatusCode = (HttpStatusCode)webex.Status;
                            }
                        }
                        if (errorStatusCode == 0)
                        {
                            errorStatusCode = (HttpStatusCode)HttpUtils.Error_UnknownError;
                        }
                        SetHttpStatus(pageUrl, errorStatusCode);
                        Console.WriteLine("Error:" + pageUrl.ToString() + " [" + (errorStatusCode != (HttpStatusCode)HttpUtils.Error_UnknownError ? HttpStatusOrErrorCodeToString(errorStatusCode) : ex.RecursiveEnumeration(x => x.InnerException).Last().Message) + "]");

                        await Task.Delay(500);
                    }
                }
                // TODO: crawl CSS for file system mode
                if (page != null)
                {
                    CrawlAndFixupLinks(path, page, pageUrlUrl);


                    SetStatus(pageUrl, UrlStatus.Crawled);
                }



            }
            else
            {
                SetStatus(pageUrl, UrlStatus.Skipped);
            }
            runningDownloads--;
            if (hasAwaited)
            {
                mainprogress?.Report(new SimpleProgress(processed, tocrawl.Count + processed));
                db.IncrementChangeCountAndMaybeSave();
            }
        }

        private static Task CreateTask(Func<Task> p)
        {
            return p();
        }

        private async Task PerformInterRequestDelayAsync()
        {
            if (InterRequestDelay.Ticks != 0)
                await Task.Delay(InterRequestDelay);
        }

        private void AddMeta(HtmlNode head, string name, string value)
        {
            var meta = head.OwnerDocument.CreateElement("meta");

            meta.SetAttributeValue("name", name);
            meta.SetAttributeValue("content", value);
            head.AppendChild(meta);
        }


        public void CrawlLinks(HtmlNode page)
        {
            var url = page.OwnerDocument.PageUrl;
            CrawlAndFixupLinks(null, page, url, force: true);
            SetStatus(url, UrlStatus.Crawled);
        }

        private void CrawlAndFixupLinks(string path, HtmlNode page, Uri pageurl, bool force = false)
        {
            db.Content.Urls2.TryGetValue(pageurl.AbsoluteUri, out var s);
            var shouldScrape = ShouldScrape(pageurl, IsPrerequisite(s));
            if (!force && shouldScrape == false) return;
            var shouldAddLinksToCrawl = shouldScrape == true;


            for (int i = 0; i < _collectAdditionalLinks.Count; i++)
            {
                var collect = _collectAdditionalLinks[i];
                var r = collect(pageurl, page);
                if (r != null)
                {
                    foreach (var (url, prereq) in r)
                    {
                        if (url != null && (shouldAddLinksToCrawl || prereq))
                            AddToCrawl(url, prereq);
                    }
                }
            }

            var elements = page.DescendantsAndSelf();
            foreach (var element in elements)
            {
                var style = element.GetAttributeValue("style");
                if (style != null && style.Contains("url("))
                {
                    CrawlCss(".dummy{" + style + "}", pageurl);
                }
                HandleAttribute(element, "href", path, shouldAddLinksToCrawl);
                HandleAttribute(element, "src", path, shouldAddLinksToCrawl);
                if (element.TagName == "img")
                {
                    foreach (var attr in element.Attributes)
                    {
                        HtmlAttribute orig;
                        if (OutputAsWarc)
                        {
                            orig = attr;
                        }
                        else
                        {
                            if (attr.Name.StartsWith("s-orig-")) continue;
                            if (attr.Name == "src") continue;
                            orig = element.Attributes["s-orig-" + attr.Name];
                            if (orig.Name == null) orig = element.Attributes[attr.Name];
                        }

                        var img = HttpExtensionMethods.TryGetImageUrl(element, orig);
                        if (img != null)
                        {
                            HandleAttribute(element, attr.Name, path, shouldAddLinksToCrawl);
                            if (!OutputAsWarc)
                            {
                                if (element.GetAttributeValue("s-orig-src") == null)
                                {
                                    var oldsrc = element.GetAttributeValue("src");
                                    element.SetAttributeValue("s-orig-src", orig.Value);
                                }
                                element.SetAttributeValue("src", element.GetAttributeValue(attr.Name));
                            }
                        }

                    }


                }
            }


            if (!OutputAsWarc)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path));
                var filestream = File.Open(path, FileMode.Create, FileAccess.Write, FileShare.Delete);
                lock (currentFiles) { currentFiles.Add(filestream); }
                try
                {
                    using (filestream)
                    using (var w = new StreamWriter(filestream, Encoding.UTF8))
                    {
                        page.OwnerDocument.WriteTo(w, false);
                    }
                    var date = page.FindSingle("meta[name='shaman-original-date']");
                    if (date != null)
                    {
                        File.SetLastWriteTimeUtc(path, DateTime.Parse(date.GetAttributeValue("content"), CultureInfo.InvariantCulture));
                    }
                }
                finally
                {
                    lock (currentFiles) { currentFiles.Remove(filestream); }
                }
            }
        }

        private void CrawlCss(string style, Uri pageurl)
        {
            var css = new ExCSS.Parser();
            var ast = css.Parse(style);
            foreach (var rule in ast.Rules)
            {
                ProcessRule(rule, pageurl);
            }
        }



        private void ProcessPrimitiveTerm(Uri baseUrl, PrimitiveTerm pv)
        {
            if (pv.PrimitiveType == UnitType.Uri && DownloadCssExternalImages)
            {
                var str = pv.Value as string;
                if (str != null && !str.StartsWith("data:"))
                {

                    try
                    {
                        var url = HttpUtils.GetAbsoluteUri(baseUrl, str);
                        if (HttpUtils.IsHttp(url))
                            AddToCrawl(url, true);
                    }
                    catch
                    {
                    }

                }
            }
        }

        private void ProcessRule(RuleSet rule, Uri baseUrl)
        {
            switch (rule.RuleType)
            {
                case RuleType.Unknown:
                    break;
                case RuleType.Style:
                    var sr = (StyleRule)rule;

                    foreach (var item in sr.Declarations)
                    {
                        var pv = item.Term as PrimitiveTerm;
                        if (pv != null) ProcessPrimitiveTerm(baseUrl, pv);
                        else
                        {
                            var termlist = item.Term as TermList;
                            if (termlist != null)
                            {
                                for (int i = 0; i < termlist.Length; i++)
                                {
                                    var p = termlist[i] as PrimitiveTerm;
                                    if (p != null) ProcessPrimitiveTerm(baseUrl, p);
                                }
                            }
                        }
                    }
                    break;
                case RuleType.Charset:
                    break;
                case RuleType.Import:
                    var ir = (ImportRule)rule;
                    var url = HttpUtils.GetAbsoluteUriAsString(baseUrl, ir.Href);
                    AddToCrawl(url, true);
                    break;
                case RuleType.Media:
                    var m = (MediaRule)rule;
                    foreach (var sub in m.RuleSets)
                    {
                        ProcessRule(sub, baseUrl);
                    }
                    break;
                case RuleType.FontFace:
                    break;
                case RuleType.Page:
                    break;
                case RuleType.Keyframes:
                    break;
                case RuleType.Keyframe:
                    break;
                case RuleType.Namespace:
                    break;
                case RuleType.CounterStyle:
                    break;
                case RuleType.Supports:
                    break;
                case RuleType.Document:
                    break;
                case RuleType.FontFeatureValues:
                    break;
                case RuleType.Viewport:
                    break;
                case RuleType.RegionStyle:
                    break;
                default:
                    break;
            }
        }




        public HtmlNode TryGetSavedPage(Uri pageUrl)
        {
            var path = GetPath(pageUrl);
            if (path == null) return null;
            return TryGetSavedPage(pageUrl, path);
        }

        private HtmlNode TryGetSavedPage(Uri pageUrl, string path)
        {

            if (OutputAsWarc) throw new NotImplementedException();
            if ((path.EndsWith(".html") || path.EndsWith(".htm")) && File.Exists(path))
            {
                var doc = new HtmlDocument();
                using (var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete))
                using (var tr = new StreamReader(fs, Encoding.UTF8, true))
                {
                    doc.Load(tr);
                }
                doc.SetPageUrl(pageUrl);
                return doc.DocumentNode;
            }
            return null;
        }



        private Uri HandleAttribute(HtmlNode element, string v, string path, bool shouldAddLinksToCrawl)
        {
            var hrefOrig = OutputAsWarc ? (null, null) : TryGetLinkUrlInternal(element, "s-orig-" + v);
            var href = hrefOrig.url == null ? TryGetLinkUrlInternal(element, v) : (null, null);
            var prerequisite = v != "href" || (element.TagName == "link" && "stylesheet".Equals(element.GetAttributeValue("rel"), StringComparison.OrdinalIgnoreCase));

            if (href.url != null || hrefOrig.url != null)
            {
                var u = hrefOrig.url ?? href.url;
                var hrefUrl = Normalize(u, element.OwnerDocument);

                var addToCrawl = HttpUtils.IsHttp(hrefUrl) && (shouldAddLinksToCrawl || prerequisite);


                if (element.TagName == "source")
                {
                    var firstSource = element.ParentNode.ChildNodes.First(x => x.TagName == "source");
                    if (element != firstSource) addToCrawl = false;
                }
                if (element.TagName == "iframe" || element.TagName == "frame")
                {
                    prerequisite = false;
                }

                if (OutputAsWarc)
                {
                    if (addToCrawl)
                    {
                        AddToCrawl(hrefUrl, prerequisite);
                    }
                }
                else
                {

                    var linkPath = GetPath(hrefUrl);
                    var hrefUrlAbs = hrefUrl.AbsoluteUri;
                    if (linkPath != null)
                    {
                        if (hrefOrig.url == null)
                            element.SetAttributeValue("s-orig-" + v, href.str);
                        if (addToCrawl)
                        {
                            AddToCrawl(hrefUrl, prerequisite);
                        }
                        element.SetAttributeValue(v, GetRelativePath(path, linkPath) + u.Fragment);
                    }
                    else
                    {


                        if (HttpUtils.IsHttp(hrefUrl))
                        {
                            var originalStr = hrefOrig.str ?? href.str;

                            if (!originalStr.StartsWith("http:") && !originalStr.StartsWith("https:"))
                                element.SetAttributeValue(v, hrefUrl.AbsoluteUri);

                            if (addToCrawl)
                                AddToCrawl(hrefUrl, prerequisite);
                        }
                    }
                }
                return hrefUrl;
            }
            return null;
        }

        public static Uri TryGetLinkUrl(HtmlNode node, string attribute)
        {
            var orig = TryGetLinkUrlInternal(node, "s-orig-" + attribute);
            if (orig.url != null) return orig.url;
            return TryGetLinkUrlInternal(node, attribute).url;
        }



        private static (Uri url, string str) TryGetLinkUrlInternal(HtmlNode node, string attribute)
        {
            if (node == null) throw new ArgumentNullException();
            try
            {
                var href = node.GetAttributeValue(attribute);
                if (href != null)
                {
                    var baseUrl = node.OwnerDocument.GetLazyBaseUrl();
                    return (HttpUtils.GetAbsoluteUrlInternal(baseUrl, href), href);
                }
            }
            catch
            {
            }
            return (null, null);
        }




        public string GetPath(Uri url)
        {
            if (OutputAsWarc) throw new InvalidOperationException();
            if (!HttpUtils.IsHttp(url)) return null;

            db.Content.Urls2.TryGetValue(url.AbsoluteUri, out var s);
            if (ShouldScrape(url, IsPrerequisite(s)) == false) return null;

            return GetPathInternal(DestinationDirectory, url);

        }


        public Func<Uri, bool, bool?> ShouldScrape { get; set; }


        public Uri Normalize(Uri url, HtmlDocument doc = null)
        {
            if (url.Scheme == "javascript")
            {
                url = TryReadJavaScriptLink(doc, url.AbsoluteUri) ?? url;
            }


            if (RewriteLink != null) url = RewriteLink(url) ?? url;
            if (!string.IsNullOrEmpty(url.Fragment) && !url.Fragment.StartsWith("#$"))
            {
                url = url.GetLeftPart_UriPartial_Query().AsUri();
            }
            return url;
        }

        private static Uri TryReadJavaScriptLink(HtmlDocument document, string javascript)
        {
            var l = javascript.IndexOf("('");
            if (l == -1) l = int.MaxValue;

            var a = javascript.IndexOf("(\"");
            if (a == -1) a = int.MaxValue;

            a = Math.Min(a, l);
            if (a != int.MaxValue)
            {
                try
                {
                    var z = (string)((JValue)HttpUtils.ReadJsonToken(javascript, a + 1)).Value;
                    return HttpUtils.GetAbsoluteUri(document.BaseUrl, z);
                }
                catch
                {
                    return null;
                }
            }

            return null;

        }

        public Encoding WebSiteEncoding { get; set; }
        public long? MaxSize { get; set; }
        public TimeSpan DatabaseSaveInterval { get; set; } = TimeSpan.FromMinutes(10);


#if WARC
        private Dictionary<string, WarcWriter> openWarcs = new Dictionary<string, WarcWriter>();
#endif

        public string Cookies { get; set; }

        private List<Func<Uri, HtmlNode, IEnumerable<(Uri, bool)>>> _collectAdditionalLinks = new List<Func<Uri, HtmlNode, IEnumerable<(Uri, bool)>>>();
        public TimeSpan InterRequestDelay { get; set; }

        public event Func<Uri, HtmlNode, IEnumerable<(Uri, bool)>> CollectAdditionalLinks
        {
            add { _collectAdditionalLinks.Add(value); }
            remove { _collectAdditionalLinks.Remove(value); }
        }

        private void CheckDisposed()
        {
            if (disposed.Task.IsCompleted) throw new ObjectDisposedException(nameof(WebsiteScraper));
        }

        public void Dispose()
        {
            if (Environment.CurrentManagedThreadId != threadId)
            {
                syncCtx.Post(() => Dispose());
                disposed.Task.GetAwaiter().GetResult();
            }
            else
            {
                if (disposed.Task.IsCompleted) return;
#if WARC
                CloseWarcs();
#endif
                if (isDbDirty)
                {
                    SaveDatabase();
                }
                lock (currentFiles)
                {
                    foreach (var file in currentFiles)
                    {
                        var path = file.Name;
                        file.Dispose();
                        File.Delete(path);
                    }
                    currentFiles.Clear();
                }
#if WARC
                lock (this)
                {
                    CloseWarcs();
                    AppendNewItemsToIndex();
                }
#endif

#if WARC
                ArchivingHttpClientHandler.Dispose();
                ArchivingHttpClient.Dispose();
#endif
                db = null;
                var p = lockFile?.Name;
                lockFile?.Dispose();
                lockFile = null;
                if (p != null) File.Delete(p);
                disposed.TrySetResult(true);
                cts?.Cancel();
            }
        }

        private FileStream lockFile;
        [Configuration(CommandLineAlias = "ignore-ssl")]
        private static bool Configuration_IgnoreSsl;
#if WARC
        private void AppendNewItemsToIndex()
        {
            lock (this)
            {
                foreach (var warc in openWarcs.Values.OrderBy(x => Path.GetFileName(x.WarcName)))
                {
                    AppendNewItemsToIndex(warc);
                }
            }
        }

        private void AppendNewItemsToIndex(WarcWriter warc)
        {
            lock (this)
            {
                if (warc.recordedResponses.Count != 0)
                {
                    WarcCdxItemRaw.AppendCdx(DestinationDirectory ?? throw new ArgumentNullException(), warc.recordedResponses);
                    warc.recordedResponses.Clear();
                }
            }
        }

        public Task<HttpResponseMessage> GetAsync(Uri url)
        {
            return url.GetAsync(MakeNewWebRequestOptions());
        }
        public Task<string> GetStringAsync(Uri url)
        {
            return url.GetStringAsync(MakeNewWebRequestOptions());
        }
        public Task<HtmlNode> GetHtmlNodeAsync(Uri url)
        {
            return url.GetHtmlNodeAsync(MakeNewWebRequestOptions());
        }
        public Task<T> GetJsonAsync<T>(Uri url)
        {
            return url.GetJsonAsync<T>(MakeNewWebRequestOptions());
        }


        private WebRequestOptions MakeNewWebRequestOptions()
        {
            return new WebRequestOptions()
            {
                AllowCachingEvenWithCustomRequestOptions = true,
                CustomHttpClient = ArchivingHttpClient,
                Cookies = this.Cookies,
                HtmlRetrieved = (page, url, ex) =>
                {
                    ((CurlWarcHandler)this.ArchivingHttpClientHandler).OnHtmlRetrieved(page, url, ex);
                }
            };
        }
#endif

        [ProtoContract]
        public class CrawlerStatus
        {
            //[ProtoMember(1)]
            //public Dictionary<string, UrlStatus> Urls;
            [ProtoMember(2)]
            public Dictionary<string, ushort> Urls2;

            [ProtoMember(3)]
            public Dictionary<string, string> Properties;
        }


#if WARC

        [Configuration]
        public static readonly string Configuration_ProxyHost;// = "localhost";
        [Configuration]
        public static readonly int Configuration_ProxyPort = 8888;


        internal static async Task<(HttpStatusCode, CurlCode, WarcItem)> ScrapeAsync(CurlEasy easy, HttpRequestMessage requestMessage, string url, MemoryStream requestMs, MemoryStream responseMs, Func<CurlEasy, WarcWriter> getWriter, object syncObj, CancellationToken ct)
        {
            if (Configuration_ProxyHost != null)
            { 
                easy.Proxy = Configuration_ProxyHost;
                easy.ProxyPort = Configuration_ProxyPort;
            }
            requestMs.SetLength(0);
            responseMs.SetLength(0);
            var startDate = DateTime.UtcNow;
            DateTime headerArrivalTime = default(DateTime);

            var hash = url.IndexOf('#');
            if (hash != -1)
                url = url.Substring(0, hash);

            easy.UserAgent = WebRequestOptions.DefaultOptions.UserAgent;
            easy.Url = url;

            easy.HeaderFunction = (byte[] buf, int size, int nmemb, Object extraData) =>
            {
                headerArrivalTime = DateTime.UtcNow;
                responseMs.Write(buf, 0, size * nmemb);
                return size * nmemb;
            };
            easy.DebugFunction = (CurlInfoType infoType, String message, int size, Object extraData) =>
            {

                if (infoType == CurlInfoType.HeaderOut)
                {
                    for (int i = 0; i < size; i++)
                    {
                        requestMs.WriteByte((byte)message[i]);
                    }
                    return;
                }
                if (infoType == CurlInfoType.HeaderIn || infoType == CurlInfoType.DataIn || infoType == CurlInfoType.SslDataIn || infoType == CurlInfoType.SslDataOut) return;


                //Console.WriteLine($"{infoType}: {message}");



            };
            easy.SslContextFunction = OnSslContext;
            easy.Verbose = true;

            if (Configuration_IgnoreSsl)
            {
                easy.CaPath = null;
                easy.SslVerifyhost = false;
                easy.SslVerifyPeer = false;

            }
            else
            {

                easy.CaInfo = ConfigurationManager.CombineRepositoryOrEntrypointPath("Shaman.Scraping/curl-ca-bundle.crt");
            }

    
            easy.WriteFunction = (byte[] buf, int size, int nmemb, Object extraData) =>
            {
                if (ct.IsCancellationRequested) return 0;
                responseMs.Write(buf, 0, size * nmemb);
                return size * nmemb;
            };
            easy.SetOpt(CurlOption.HttpTransferDecoding, false);
            easy.SetOpt(CurlOption.HttpContentDecoding, false);
            easy.MaxRedirs = 0;
            easy.ReadFunction = null;

            easy.CustomRequest = null;
            Stream requestContent = null;
            LazyUri shamanUrl = null;


            var curlRequestHeaders = new CurlSlist();
            curlRequestHeaders.Append("Expect:");


            if (requestMessage != null)
            {

                {
                    foreach (var item in (IEnumerable<KeyValuePair<string, string>>)requestMessage.Headers.InvokeFunction("GetHeaderStrings"))
                    {
                        curlRequestHeaders.Append(item.Key + ": " + item.Value);
                    }
                }

                requestMessage.Properties.TryGetValue("ShamanURL", out var shamanUrlObj);
                shamanUrl = shamanUrlObj as LazyUri;

                if (requestMessage.Content != null)
                {
                    foreach (var item in (IEnumerable<KeyValuePair<string, string>>)requestMessage.Content.Headers.InvokeFunction("GetHeaderStrings"))
                    {
                        curlRequestHeaders.Append(item.Key + ": " + item.Value);
                    }
                }


                easy.CustomRequest = requestMessage.Method.ToString().ToUpperInvariant();
                if (requestMessage.Content != null)
                {
                    requestContent = await requestMessage.Content.ReadAsStreamAsync();
                    var len = requestContent.Length;
                    easy.Post = true;
                    curlRequestHeaders.Append("Content-Length: " + len);
                    easy.ReadFunction = (byte[] buf, int size, int nmemb, object extraData) =>
                    {
                        return requestContent.Read(buf, 0, size * nmemb);
                    };
                }
            }

            curlRequestHeaders.Append("Connection: keep-alive");
            if (!curlRequestHeaders.Strings.Any(x => x.StartsWith("Accept-Language:")))
            {
                curlRequestHeaders.Append("Accept-Language: en-US, en; q=0.5");
            }

            easy.HttpHeader = curlRequestHeaders;
            // easy.Proxy = "http://localhost:8888";
            CurlCode code = default(CurlCode);
            await Task.Run(() => code = easy.Perform());
            ct.ThrowIfCancellationRequested();
            WarcItem warcItem = null;
            if (code == CurlCode.Ok)
            {
                if (requestContent != null)
                {
                    requestContent.Seek(0, SeekOrigin.Begin);
                    await requestContent.CopyToAsync(requestMs);
                }
                string ip = null;
                easy.GetInfo((CurlInfo)1048608 /*CURLINFO_PRIMARY_IP*/, ref ip);
                var requestId = Guid.NewGuid().ToString();
                var responseId = Guid.NewGuid().ToString();
                lock (syncObj ?? new object())
                {
                    var dest = getWriter(easy);
                    if (dest != null)
                    {
                        lock (dest)
                        {
                            dest.WriteRecord(url, false, requestMs, startDate, ip, requestId, null, shamanUrl);
                            warcItem = dest.WriteRecord(url, true, responseMs, headerArrivalTime, ip, responseId, requestId, shamanUrl);
                            warcItem.WarcFile = dest.FullName;
                        }
                    }
                }
            }


            return ((HttpStatusCode)easy.ResponseCode, code, warcItem);
        }


        private static CurlCode OnSslContext(CurlSslContext ctx, object extraData)
        {
            return CurlCode.Ok;
        }

#endif


        public string[] Rules { get; set; }


#if DOM_EMULATION

        public Func<DomEmulator, Task<bool>> OnPageInteract { get; set; }


        public async Task<bool> EmulateDomAsync(Uri url)
        {
            this.PerformInitialization();
            if (this.CdxIndex.TryGetValue(url.AbsoluteUri, out var warc))
            {
                if (warc.ContentType != null && !warc.ContentType.Contains("html")) return false;
            }
            HtmlNode page;
            try
            {
                page = await this.GetHtmlNodeAsync(url);
            }
            catch (NotSupportedResponseException)
            {
                return false;
            }
            using (var emulator = new DomEmulator(page.OwnerDocument))
            {
                emulator.CustomHttpClient = this.ArchivingHttpClient;
                if (await OnPageInteract(emulator) && !emulator.HadHttpErrors)
                {
                    SetDomEmulated(url.AbsoluteUri);
                    return true;
                }
            }
            return false;
        }



#endif

    }

    internal static class ScraperUtils
    {

        public static bool IsMatchSimple(this Uri url, string model)
        {
            return IsMatchSimple(url.AbsoluteUri, model);
        }

        public static bool IsMatchSimple(this string url, string model)
        {
            Regex r;
            if (model == "**") return true;
            lock (UrlModelCache)
            {
                r = UrlModelCache.TryGetValue(model);
                if (r == null)
                {
                    var a = model.Replace("**", "__starstar__").Replace("*", "__star__");
                    var regex = ("^" + Regex.Escape(a) + "$")
                        .Replace("__starstar__", @".*")
                        .Replace("__star__", @"[^/\?&]+");
                    r = new Regex(regex);
                    UrlModelCache[model] = r;
                }
            }
            return r.IsMatch(url);
        }

        private static Dictionary<string, Regex> UrlModelCache = new Dictionary<string, Regex>();


        public static Task<T> WithCancellation<T>(this Task<T> task, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<T>();
            var d = ct.Register(() =>
            {
                tcs.TrySetCanceled();
            });
            task.GetAwaiter().OnCompleted(() =>
            {
                if (task.IsCanceled) tcs.TrySetCanceled();
                else if (task.IsFaulted) tcs.TrySetException(task.Exception);
                else tcs.TrySetResult(task.Result);
                d.Dispose();
            });
            return tcs.Task;
        }

        class PreviousStringInfo
        {
            public string PreviousString;
        }

        private static Dictionary<IProgress<SimpleProgress>, PreviousStringInfo> progresses = new Dictionary<IProgress<SimpleProgress>, PreviousStringInfo>();

        public static void Report(this IProgress<SimpleProgress> progress, string status)
        {
            if (progress == null) return;
            if (!Console.IsOutputRedirected)
            {
                lock (progresses)
                {

                    var z = progresses.TryGetValue(progress);
                    if (z == null)
                    {
                        z = new PreviousStringInfo();
                        progresses[progress] = z;
                    }
                    var maxwidth = Console.WindowWidth - 5;
                    if (status.Length > maxwidth)
                    {
                        var truncated1 = status.TrimSize(maxwidth, 0, false);
                        var truncated2 = z.PreviousString?.TrimSize(maxwidth, 0, false);
                        z.PreviousString = status;
                        if (truncated1 == truncated2)
                        {
                            var firstHalf = status.Substring(0, maxwidth / 2);
                            status = firstHalf + "…" + status.Substring(status.Length - maxwidth / 2);
                        }
                    }
                    else
                    {
                        z.PreviousString = status;
                    }


                }
            }
            progress.Report(new SimpleProgress(status));
        }

        public static bool HasNoQueryParameters(this Uri url)
        {
            return url.Query.Length <= 1;
        }
        public static bool HasQueryParameters(this Uri url)
        {
            return url.Query.Length > 1;
        }



        public static bool HasExactlyQueryParameters(this Uri url, params string[] parameters)
        {
            if (parameters.Length != 0 && url.Query.Length <= 1) return false;
            var idx = 0;
            foreach (var item in url.GetQueryParameters())
            {
                if (idx == parameters.Length) return false;
                if (item.Key == parameters[idx]) idx++;
                else return false;
            }
            return idx == parameters.Length;
        }

        public static bool PathContains(this Uri url, string str)
        {
            return url.AbsolutePath.Contains(str);
        }

        private static string ConcatCached(string a, string b)
        {
            var sb = ReseekableStringBuilder.AcquirePooledStringBuilder();
            sb.Append(a);
            sb.Append(b);
            var z = sb.ToStringCached();
            ReseekableStringBuilder.Release(sb);
            return z;
        }

        public static bool PathContainsComponent(this Uri url, string str)
        {
            var z = str;
            if (!z.StartsWith("/")) z = ConcatCached("/", str);
            if (z.EndsWith("/")) z = z.SubstringCached(0, z.Length - 1);
            if (url.AbsolutePath.EndsWith(z)) return true;
            if (url.AbsolutePath.Contains(ConcatCached(z, "/"))) return true;
            return false;
            //if(url.AbsolutePath.Contains(str + "/"))
        }
        public static bool PathEndsWith(this Uri url, string str)
        {
            return url.AbsolutePath.EndsWith(str);
        }

        public static bool Contains(this Uri url, string str)
        {
            return url.AbsoluteUri.Contains(str);
        }

        public static bool StartsWith(this Uri url, string str)
        {
            return url.AbsoluteUri.StartsWith(str);
        }

        public static bool PathStartsWith(this Uri url, string str)
        {
            return url.AbsolutePath.StartsWith(str);
        }
        public static bool EndsWith(this Uri url, string str)
        {
            return url.AbsoluteUri.EndsWith(str);
        }

    }


    public enum UrlStatus : byte
    {
        ToCrawl,
        Crawled,
        Skipped,
        Processing,
        Error,
        Redirect,
        Downloaded,
        UnknownUrl,
        DomEmulated,
        RetrievedButNotCrawled
    }


}
