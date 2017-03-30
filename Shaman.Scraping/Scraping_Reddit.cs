using Shaman.Runtime;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Shaman.Scraping
{
    public class RedditScraper : WebsiteScraper
    {

        public bool DownloadComments = true;
        public bool DownloadPdfs = true;
        public bool DownloadGifs = true;
        public string Subreddit;

        protected override void Initialize()
        {
            //this.UrlPriorityDelegate = x => (x.StartsWith("https://www.reddit.com/") ? 1000 : 1000) + x.Length;

            this.Cookies = "over18=1";
            if (Subreddit == null) throw new ArgumentNullException("Subreddit");
            var root = ("https://www.reddit.com/r/" + Subreddit + "/top/?sort=top&t=all").AsUri();

            var subreddit = root.GetPathComponent(1);
            this.DestinationSuggestedName = (root.IsHostedOn("voat.co") ? "voat" : "reddit") + "-" + subreddit;

            this.CollectAdditionalLinks += (pageurl, doc) =>
            {
                return doc.DescendantsAndSelf().Select(x => x.GetAttributeValue("data-url")).Where(x => x != null && (x.Contains("reddituploads.com") || x.Contains("redd.it"))).Select(x => (x.AsUri(), false));
            };
            var httpsWwwNormalized = CreateHttpsWwwNormalizer(root, root.IsHostedOn("reddit.com"));
            this.RewriteLink = url =>
            {
                var u = httpsWwwNormalized(url);
                if (u != null) return u;
                if (url.IsHostedOn("imgur.com") && GetExtension(url) == ".gif" && url.GetQueryParameter("gif") != "1" && !url.AbsolutePath.StartsWith("/images/loaders"))
                {
                    return (url.GetLeftPart(UriPartial.Authority) + url.PathAndQuery.Replace(".gif", ".mp4")).AsUri();
                }
                return null;
            };

            this.ShouldScrape = (url, isPrerequisite) =>
            {
                if (url.IsHostedOn("minus.com")) return false; // Dead site.
                if (url.IsHostedOn("i.sli.mg")) return false; // Dead site.
                if (url.IsHostedOn("embedly.com")) return false;
                if (url.IsHostedOn("redditmedia.com") && url.AbsolutePath.StartsWith("/mediaembed/")) return false;
                if (url.AbsolutePath.Contains("/css/css/")) return false;
                if (url.AbsolutePath.Contains("/javascript/javascript/")) return false;
                if (url.IsHostedOn("viralchronics.com")) return false;
                if (url.IsHostedOn("thedoghousediaries.com"))
                {
                    if (url.AbsolutePath.StartsWith("/large/javascript/")) return false;
                    if (url.AbsolutePath.StartsWith("/large/logos/")) return false;
                    if (url.AbsolutePath.StartsWith("/large/dhdcomics/")) return false;
                    if (url.AbsolutePath.StartsWith("/large/css/")) return false;
                }


                var ext = GetExtension(url);
                if (!DownloadGifs && (ext == ".mp4" || ext == ".gif" || ext == ".webm" || ext == ".gifv")) return false;

                if (isPrerequisite) return true;
                if (url == root) return true;

                if (url.IsHostedOn("imgur.com"))
                {
                    if (url.AbsolutePath.StartsWith("/memegen/create")) return false;
                    return null;
                }
                if (url.IsHostedOn("i.redd.it")) return null;
                if (url.IsHostedOn("reddituploads.com")) return null;
                if (url.IsHostedOn("i.sli.mg")) return null;
                if (url.IsHostedOn("imgflip.com")) return null;
                if (url.IsHostedOn("livememe.com")) return null;
                if (url.IsHostedOn("memegen.com")) return null;
                if (url.IsHostedOn("memegenerator.net")) return null;
                if (url.IsHostedOn("quickmeme.com")) return null;
                if (url.IsHostedOn("qkme.me")) return null;
                if (url.IsHostedOn("memedad.com")) return null;
                if (url.IsHostedOn("gfycat.com")) return null;
                if (ext == ".jpg" || ext == ".jpeg" || ext == ".gif" || ext == ".png") return null;
                if (ext == ".pdf") return DownloadPdfs ? null : (bool?)false;

                if (url.IsHostedOn("pastebin.com")) return null;
                if (url.IsHostedOn("pasted.co")) return null;
                if (url.IsHostedOn("hastebin.com")) return null;
                if (url.IsHostedOn("gist.github.com")) return null;

                if (url.IsHostedOn("reddit.com") || url.IsHostedOn("voat.co"))
                {


                    if (url.GetPathComponent(1) != subreddit) return false;
                    if (url.IsMatchSimple("https://www.reddit.com/r/*/top/?sort=top&t=all&count=*&after=*")) return true;
                    if (DownloadComments && url.IsMatchSimple("https://www.reddit.com/r/*/comments/*/*/")) return true;
                    if (url.IsMatchSimple("https://voat.co/v/*/top?page=*")) return true;
                    return false;
                }

                return false;

            };


            var old = this.GetDestinationWarc;
            this.GetDestinationWarc = (url, easy, req, res) =>
            {
                var ct = easy.ContentType;
                if (ct != null && ct.StartsWith("video/")) return "video";
                if (ct != null && ct.StartsWith("image/gif") && url.IsHostedOn("imgur.com")) return "video";
                if (url.IsHostedOn("imgur.com")) return "imgur";
                if (url.IsHostedOn("i.redd.it")) return "reddituploads";
                if (url.IsHostedOn("reddituploads.com")) return "reddituploads";
                return old(url, easy, req, res);
            };


            //foreach (var item in this.GetMatchingUrls("**comments/*/*/"))
            // {
            //    Console.WriteLine(item+": " + this.GetStatus(item));
            //}

            var altTumblrServer = "68.media.tumblr.com";

            this.OnError = (url, easy, code) =>
            {
                if (code == System.Net.HttpStatusCode.BadRequest && url.IsHostedOn("imgur.com") && url.AbsolutePath.EndsWith(".mp4"))
                {
                    this.AddToCrawl((url.GetLeftPart(UriPartial.Path).Replace(".mp4", ".gif") + "?gif=1").AsUri(), true);
                }
                if ((int)code == 806 /*CouldntResolveHost*/ && url.IsHostedOn("media.tumblr.com") && !url.IsHostedOn(altTumblrServer))
                {
                    var newurl = url.Scheme + "://" + altTumblrServer + url.PathAndQuery;
                    this.AddToCrawl(newurl, this.IsPrerequisite(url));
                }
            };

            this.AddToCrawl(root);

            //this.ReconsiderFailedUrls();
            //this.ReconsiderForScraping(@"**");

        }
    }
}