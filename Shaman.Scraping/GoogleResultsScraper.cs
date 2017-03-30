using Shaman.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shaman.Scraping
{
    public class GoogleResultsScraper : WebsiteScraper
    {
        public Uri ResultsPage;
        public int MaxResults;
        public bool DownloadGoogleCache;

        protected override void Initialize()
        {

            this.UrlPriorityDelegate = url => (url.StartsWith("www.google.") ? 1000 : 0) + url.Length;
            this.ShouldScrape = (url, prereq) =>
            {
                if (IsGoogleDomain(url) && url.PathStartsWith("/sorry/"))
                {
                    throw new Exception("Sorry.");
                }
                if (url.GetQueryParameter("start") != null && url.Host == ResultsPage.Host && prereq)
                {
                    var s = url.GetQueryParameter("start");
                    if (int.TryParse(s, out var offset))
                    {
                        if (offset >= MaxResults) return false;
                        return true;
                    }
                    return false;
                }

                if (prereq)
                {
                    if (url.Host.Contains("ebay") && url.AbsolutePath.EndsWith(".css")) return null;
                    return true;
                }

                if (HttpUtils.UrisEqual(url, ResultsPage)) return true;
                if (url.Host.StartsWith("translate.google")) return false;
                if (DownloadGoogleCache && url.Host == "webcache.googleusercontent.com") return true;

                return false;
            };
            this.CollectAdditionalLinks += (url, page) =>
            {
                if (IsGoogleDomain(url))
                {
                    return page
                        .FindAll("a")
                        .Select(x => x.TryGetLinkUrl())
                        .Where(x => x != null && !IsGoogleDomain(x) && !x.IsHostedOn("googleusercontent.com") && !x.Host.StartsWith("translate.google."))
                        .Select(x =>
                        {
                            return (x, true);
                        });
                }
                return null;
            };
            this.CollectAdditionalLinks += (url, page) =>
            {
                if (IsGoogleDomain(url))
                {
                    return page
                    .FindAll("#nav a")
                    .Select(x => x.TryGetLinkUrl())
                    .Where(x => IsGoogleDomain(x) && x.GetQueryParameter("start") != null)
                    .Select(x => (NormalizeGoogleResultsUrl(x), true));

                }
                return null;
            };
            AddToCrawl(NormalizeGoogleResultsUrl(ResultsPage));
        }

        private Uri NormalizeGoogleResultsUrl(Uri resultsPage)
        {
            var lazy = new LazyUri(resultsPage);
            lazy.RemoveQueryParameter("ei");
            lazy.RemoveQueryParameter("sei");
            return lazy.Url;
        }

        private bool IsGoogleDomain(Uri url)
        {
            if (url.IsHostedOn("google.it")) return true;
            if (url.IsHostedOn("google.com")) return true;
            if (!url.Host.Contains("google")) return false;
            if (Tld.GetDomainFromUrl(url).StartsWith("google.")) return true;
            return false;
        }
    }


}
