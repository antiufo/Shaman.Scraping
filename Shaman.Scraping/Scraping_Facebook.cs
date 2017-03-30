using Shaman.Connectors.Facebook;
using Shaman.Runtime;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Shaman.Dom;
using System.Text;
using Newtonsoft.Json.Linq;

namespace Shaman.Scraping
{
    internal class FacebookScraper : WebsiteScraper
    {

        //https://www.facebook.com/?hc_ref=NEWSFEED&fref=nf

        public string Page;
        public bool DownloadThumbnailsAndCss = true;
        public bool DownloadComments;
        public bool DownloadFullSizeImages;

        public void AddImagesToCrawl()
        {
            foreach (var item in CdxIndex.Values)
            {
                if (item.Url.Contains("/pages_reaction_units/"))
                {
                    ProcessPageReactionUnitsRequest(item.Url.AsUri(), item.OpenStream());
                }
            }
        }

        protected override void Initialize()
        {


            // this.Parallelism = Math.Min(this.Parallelism, 6);
            var p = Page;
            if (p.StartsWith("http:") || p.StartsWith("https:"))
            {
                p = p.AsUri().AbsolutePath.Trim('/');
            }
            if (p.StartsWith("groups-")) p = "groups/" + p.TrimStart("groups-");
            var root = ("https://m.facebook.com/" + p.TrimStart("/")).AsUri();


            if (this.DestinationSuggestedName == null)
            {
                this.DestinationSuggestedName = "facebook-" + (root.GetQueryParameter("id") ?? root.AbsolutePath.TrimStart("/").TrimEnd("/").Replace("/", "-"));
                if (!ScrapeMobileVersion) this.DestinationSuggestedName += "-desktop";
            }


            if (!ScrapeMobileVersion)
            {
                UrlPriorityDelegate = x => (x.Contains("/ajax/") ? -1000 : 0) + x.Length;
                root = ("https://www.facebook.com" + root.PathAndQuery).AsUri();
                this.ShouldScrape = (url, prereq) =>
                {
                    if (prereq && DownloadThumbnailsAndCss) return true;
                    if (Utils.Equals(url, root)) return true;
                    if (url.Fragment.StartsWith("#$"))
                    {
                        if (url.PathContains("GroupEntstreamPagelet")) return true;
                    }
                    if (url.PathStartsWith("/pages_reaction_units")) return true;
                    return false;
                };
                this.AddToCrawl(root);
                OnNonHtmlReceived += (url, easy, body) =>
                {
                    if (url.PathStartsWith("/pages_reaction_units"))
                    {
                        ProcessPageReactionUnitsRequest(url, body);
                    }
                };
                CollectAdditionalLinks += (url, page) =>
                {

                    if (HttpUtils.UrisEqual(url, root))
                    {
                        var fburl = page.GetValue(":property('al:android:url')");
                        var numericId = fburl.Capture(@"/(\d+)$");

                        
                        if (fburl.Contains("group/"))
                        {
                            var initialUrl = ReadFromExample("fbgroup");
                            initialUrl.AppendFragmentParameter("$json-query-data.group_id~", numericId);

                            AddComplexAjax(initialUrl.Url, "§£json-query-data.end_cursor={script:json-token('£jscc_map'):json-token('£end_cursor')}");
                        }
                        else if (fburl.Contains("page/"))
                        {
                            var initialUrl = ReadFromExample("fbpage");
                            //defaultParameters = initialUrl.FragmentParameters.ToDictionary();

                            initialUrl.AppendQueryParameter("page_id", numericId);

                            AddToCrawl(initialUrl.Url);
                        }
                        else if (fburl.Contains("profile/"))
                        {
                            var initialUrl = ReadFromExample("fbprofile");
                            initialUrl.AppendQueryParameter("profile_id", numericId);
                            AddToCrawl(initialUrl.Url);
                        }
                        else throw new NotSupportedException("Not supported: " + fburl);



                    

                    }
                    return null;
                };

                CollectAdditionalLinks += (url, page) =>
                {
                    //Console.WriteLine("Size: " + page.OuterHtml.Length);
                    var html = page.FindAll(page.OwnerDocument.IsJson() ? Configuration_PageReactionUnitsSelector : Configuration_PageletExtractionSelector);

                    return html.SelectMany(x =>
                    {
                        return x.DescendantsAndSelf().Select(y =>
                        {
                            var ajaxify = y.GetAttributeValue("ajaxify");
                            if (ajaxify != null && ajaxify.StartsWith("/pages_reaction_units/"))
                            {

                                var u = new Uri("https://www.facebook.com" + ajaxify);
                                return (Url: u, false);
                            }

                            var z = y.TryGetLinkUrl();
                            if (z != null)
                            {
                                return (Url: z, y.TagName.In("img", "script") || y.GetAttributeValue("rel")?.ToLowerFast() == "stylesheet");
                            }
                            return (null, false);
                        }).Where(y => y.Url != null);
                    });
                };

            }
            else
            {

                this.CollectAdditionalLinks += (url, page) =>
                {

                    return page.FindAll("a[href*='sectionLoadingID='],a[href*='bacr']:text-is('See More Posts')").Select(x =>
                    {
                        var u = x.TryGetLinkUrl();
                        var sb = ReseekableStringBuilder.AcquirePooledStringBuilder();
                        sb.Append(u.GetLeftPart(UriPartial.Path));
                        HttpUtils.AppendQueryParameters(u.GetQueryParameters().Select(y =>
                        {
                            var val = y.Value;
                            if (y.Key == "timecutoff") val = "0";
                            else if (y.Key == "refid") val = "0";
                            else if (y.Key == "sectionLoadingID") val = "x";
                            else if (y.Key == "yearSectionsYears") val = "";
                            return new KeyValuePair<string, string>(y.Key, val);
                        }), sb);
                        HttpUtils.AppendQueryParameter("srw", "1", sb);
                        var rewritten = ReseekableStringBuilder.GetValueAndRelease(sb);
                        return (rewritten.AsUri(), false);
                    });
                };

                this.ShouldScrape = (url, isPrerequisite) =>
                {
                    if (DownloadThumbnailsAndCss && isPrerequisite) return true;
                    if (url == root) return true;
                    if (!url.IsHostedOn("m.facebook.com")) return false;
                    if (url.GetQueryParameter("srw") != null)
                    {
                        /*
                        if (url.Query.StartsWith("?sectionLoadingID="))
                        {
                            // Click Show more only for individual years, not the global one.
                            return url.GetQueryParameter("timestart") != "0";
                        }
                        */
                        return true;
                    }

                    if (DownloadComments || DownloadFullSizeImages)
                    {
                        if (IsSubfolderOf(url, root))
                        {
                            if (url.HasNoQueryParameters() && string.IsNullOrEmpty(url.Fragment)) return true;
                            AddToCrawl(url.GetLeftPart(UriPartial.Path));
                            return false;
                        }
                    }

                    return false;
                };
                this.AddToCrawl(root);
            }
        }

        private void ProcessPageReactionUnitsRequest(Uri url, Stream body)
        {
            using (var responseText = new StreamReader(body, Encoding.UTF8))
            {
                var t = responseText.ReadToEnd();
                var els = GetHtmlPiecesFromJson(t, url);
                foreach (var q in els)
                {
                    foreach (var item in q.FindAll("[ajaxify]"))
                    {
                        var ajaxify = item.GetAttributeValue("ajaxify");
                        if (ajaxify.StartsWith("/pages_reaction_units"))
                        {
                            AddToCrawl("https://www.facebook.com" + ajaxify + "&__a=1");
                        }
                    }
                    CrawlLinks(q);
                    
                }

            }
        }

        private LazyUri ReadFromExample(string name)
        {
            string text;
            if (name == "fbgroup")
            {
                text = @"
https://www.facebook.com/ajax/pagelet/generic.php/GroupEntstreamPagelet?
ajaxpipe=1#
$json-query-data.group_id~=xxxxx&
$json-query-data.multi_permalinks~=--
";
            }
            else if (name == "fbpage")
            {
                text= @"
https://www.facebook.com/pages_reaction_units/more/?
surface=www_pages_home&
unit_count=8&
cursor={""timeline_section_cursor"":{},""has_next_page"":true}&
__a = 1
";
            }
            else
            {
                throw new NotImplementedException();
            }


            //text = File.ReadAllText(@"c:\temp\" + name + "-req.txt");
            return new LazyUri(text.RegexReplace(@"\s+", string.Empty).AsUri());
        }

        class Post
        {
            public DateTime Date;
            public string Text;
            public long Id;
        }

        [Configuration]
        private static string Configuration_PageletExtractionSelector = @"script:json-token('§respond\\(\\d+\\,') > payload > content > *:reparse-html";
        [Configuration]
        private static string Configuration_PageReactionUnitsSelector = @"__html:reparse-html";
        public bool ScrapeMobileVersion;

        public void CreatePostList()
        {
            FlushWarcs();
            var posts = new List<Post>();
            foreach (var warcItem in CdxIndex.Values)
            {
                if ((warcItem.Url.Contains("facebook.com/pages_reaction_units/") || warcItem.ContentType.Contains("html")) && warcItem.Url.Contains("facebook.com"))
                {
                    var l = new List<HtmlNode>();
                    if (warcItem.ContentType.Contains("html"))
                    {
                        var p = warcItem.ReadHtml();
                        l.Add(p);
                        l.AddRange(p.FindAll(Configuration_PageletExtractionSelector));
                    }
                    else
                    {
                        l.AddRange(GetHtmlPiecesFromJson(warcItem.ReadText(), warcItem.Url.AsUri()));
                    }

                    foreach (var article in l.SelectMany(x => x.FindAll("[role='article']")))
                    {
                        var dateel = article.FindSingle("abbr");
                        if (dateel == null) continue;
                        var datestr = dateel.GetAttributeValue("title") ?? dateel.GetText();

                        DateTime date;
                        if (!datestr.Contains("hr") && !datestr.Contains("min"))
                        {
                            date = Conversions.ParseDateTime(datestr, null, warcItem.Date);
                            //datestr = datestr.Replace("Yesterday", warcItem.Date.Date.AddDays(-1).ToString("MMM dd, yyyy"));
                            //datestr = datestr.Replace("Today", warcItem.Date.Date.ToString("MMM dd, yyyy"));
                            //date = DateTime.Parse(datestr.Replace(" at ", " "), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                        }
                        else
                        {
                            if (datestr.Contains("min"))
                            {
                                var z = datestr.TrimEnd("mins").TrimEnd("min");
                                date = warcItem.Date.AddMinutes(-int.Parse(z));
                            }
                            else
                            {
                                var z = datestr.TrimEnd("hrs").TrimEnd("hr");
                                date = warcItem.Date.AddHours(-int.Parse(z));
                            }
                        }

                        var text = article.TryGetValue("/*:without-subnodes(a,abbr,[aria-hidden=true])");

                        var postid = GetFacebookPostId(article);
                        if (!long.TryParse(postid, out var id))
                        {
                            throw new Exception("Bad post id.");
                        }
                        posts.Add(new Post() { Date = date, Id = id, Text = text });

                    }
                }
            }

            posts = posts.GroupBy(x => x.Id).Select(x => x.First()).OrderByDescending(x => x.Date).ToList();
            posts.SaveTable(Path.Combine(DestinationDirectory, "Posts.csv"));



        }

        private IEnumerable<HtmlNode> GetHtmlPiecesFromJson(string json, Uri url)
        {
            var pos = 0;
            while (true)
            {
                var k = "\"__html\":";
                pos = json.IndexOf(k, pos);
                if (pos == -1) break;
                pos += k.Length;
                var html = ((JValue)HttpUtils.ReadJsonToken(json, pos)).Value<string>();
                pos++;
                var el = html.AsHtmlDocumentNode();
                el.OwnerDocument.SetPageUrl(url);
                yield return el;
            }

        }

        private string GetFacebookPostId(HtmlNode article)
        {
            string postid;
            var abbr = article.FindSingle("abbr");
            if (abbr != null && abbr.ParentNode.TagName == "a")
            {
                var permalink = abbr.ParentNode.GetLinkUrl();
                postid = permalink.GetQueryParameter("story_fbid");
                if (postid != null) return postid;
                var components = permalink.AbsolutePath.SplitFast('/', StringSplitOptions.RemoveEmptyEntries);
                postid = components.Last();
                if (postid == "set")
                {
                    postid = permalink.GetQueryParameter("set").CaptureBetween("a.", ".");
                }
                else if (postid == "photo.php")
                {
                    postid = Photo.TryGetPhoto(permalink).Id.ToString();
                }
                //postid = permalink.AbsolutePath.CaptureBetween("/permalink/", "/");
                return postid;
            }

            var u = article.TryGetLinkUrl("a:text-is('Full Story')");

            if (u != null)
            {
                postid = u.GetQueryParameter("story_fbid");
                if (postid != null) return postid;

                var ft = u.GetQueryParameter("_ft_");
                if (ft != null)
                {
                    postid = ft.TryCaptureBetween("top_level_post_id.", ":") ?? ft.CaptureBetween("tl_objid.", ":");
                    if (postid != null) return postid;
                }
            }

            u = article.TryGetLinkUrl("h3 strong a");
            if (u != null)
            {
                var ft = u.GetQueryParameter("_ft_");
                if (ft != null)
                {
                    postid = ft.TryCaptureBetween("top_level_post_id.", ":") ?? ft.CaptureBetween("tl_objid.", ":");
                    if (postid != null) return postid;
                }
            }

            u = article.TryGetLinkUrl("a:contains('React')");
            if (u != null)
            {
                postid = u.GetQueryParameter("ft_id");
                if (postid != null) return postid;
            }


            u = article.TryGetLinkUrl("a:contains('See More Photos')");
            if (u != null)
            {
                postid = u.AbsolutePath.TryCaptureBetween("/albums/", "/");
                if (postid != null) return postid;
            }


            var photo = article.DescendantsAndSelf()
                .Where(x =>
                {
                    var src = x.GetAttributeValue("src");
                    if (src != null)
                    {
                        if (src.Contains("50x50")) return false;
                    }
                    return true;
                })
                .Select(x => Photo.TryGetPhoto(x.TryGetLinkUrl())).FirstOrDefault(x => x != null);
            postid = photo?.Id.ToString();
            if (postid != null) return postid;

            throw new Exception();

        }


    }
}