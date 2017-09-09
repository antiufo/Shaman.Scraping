using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Shaman.Runtime;
#if SHAMAN
using Shaman.Annotations;
using Newtonsoft.Json.Linq;
using Shaman.Connectors.Blogs;
#endif
using System.IO;
using System.Threading;
using Shaman.Types;
using Shaman.Dom;
using System.Net.Http;

namespace Shaman.Connectors.Facebook
{
    public class Photo
#if SHAMAN
        : Entity
#endif
    {

#if SHAMAN
        [Key]
#endif
        public long Id;

        public static Photo TryGetPhoto(Uri url)
        {
            var id = TryGetPhotoId(url);
            if (id == null) return null;
#if SHAMAN
            return ObjectManager.GetEntityByKeyValue<Photo>(id);
#else
            return new Photo() { Id = id.Value };
#endif
        }

#if SHAMAN
        [Extraction(DetailLevel.D)]
        public WebImage LargestImage;
#else
        public WebFile LargestImage;
#endif

#if SHAMAN
        [Extraction(DetailLevel.D)]
#endif
        public DateTime? Date;

#if !SHAMAN
        public static Func<Uri, Task<HtmlNode>> GetNodeAsync;
#endif

#if SHAMAN
        [RetrieveDetailsMethodInfo(DetailLevel.D)]
        protected async override Task RetrieveDetailsAsync(DetailLevel detailLevel)
        {
            if (detailLevel == DetailLevel.D)
            {
#else
        public async Task LoadDetailsAsync(HttpClient client = null)
        {
            {
#endif
                var l = new LazyUri("https://m.facebook.com/photo.php?fbid=" + Id);
#if SHAMAN

                l.AppendFragmentParameter("$cookie-c_user", Blog.Configuration_FacebookUserId.ToString());
                l.AppendFragmentParameter("$cookie-xs", Blog.Configuration_FacebookXs);
                var page = await l.GetHtmlNodeAsync();
#else
                HtmlNode page;
                if (client != null)
                {
                    page = await l.Url.GetHtmlNodeAsync(new WebRequestOptions() { CustomHttpClient = client, AllowCachingEvenWithCustomRequestOptions = true });
                }
                else
                {
                    page = await GetNodeAsync(l.Url);
                }
#endif
                var url = page.GetLinkUrl("a:text-is('View Full Size')");
#if SHAMAN
                LargestImage = WebImage.FromUrlUntracked(url);
#else
                LargestImage = WebFile.FromUrl(url);
#endif

                Date = Conversions.TryParseDateTime(page.TryGetValue("abbr"), null, false, null);

                /*
                var k = await ("https://graph.facebook.com/" + Id + "?fields=images,from,created_time,backdated_time&access_token=" + Utils.EscapeDataString(Blog.Configuration_FacebookUserAccessToken)).AsLazyUri().GetJsonAsync<JObject>();

                var img = ((JArray)k["images"]).MaxByOrDefault(x => ((JObject)x).Value<int>("height"));
                LargestImage = WebImage.FromUrl(img.Value<string>("source").AsUri());
                var backdated = img.Value<string>("backdated_time");
                var created = img.Value<string>("created_time");

                if (created != null) DateCreated = Conversions.ParseDateTime(created, null, null);
                if (backdated != null) DateBackdated = Conversions.ParseDateTime(backdated, null, null);
                */



            }
        }

        public async Task<string> DownloadAsync(string folder)
        {
            var dest = System.IO.Path.Combine(folder, Id + ".jpg");
            if (File.Exists(dest)) return dest;
#if SHAMAN
            await this.LoadDetailsAsync(DetailLevel.D);
#else
            await this.LoadDetailsAsync();
#endif
            await LargestImage.DownloadAsync(folder, Id + ".jpg", WebFile.FileOverwriteMode.Skip, CancellationToken.None, null);
            var httpDate = File.GetLastWriteTimeUtc(dest);
            if (this.Date != null && Math.Abs((httpDate - this.Date.Value).TotalDays) >= 2)
                File.SetLastWriteTimeUtc(dest, this.Date.Value);
            return dest;
        }

        public async Task<string> DownloadToBlobStoreAsync(string blobStoreFolder)
        {
            var dest = System.IO.Path.Combine(blobStoreFolder, Id + ".jpg");
            if (BlobStore.Exists(dest)) return dest;
#if SHAMAN
            await this.LoadDetailsAsync(DetailLevel.D);
#else
            await LoadDetailsAsync();
#endif

            using (var stream = await this.LargestImage.OpenStreamAsync())
            {
                DateTime? httpDate = null;
                if (stream is MediaStream ms)
                {
                    httpDate = (await ms.Manager.GetResponseAsync()).Content.Headers.LastModified?.UtcDateTime;
                }
                if (httpDate == null || (this.Date != null && Math.Abs((httpDate.Value - this.Date.Value).TotalDays) >= 2))
                    httpDate = this.Date;
                using (var destStream = BlobStore.OpenWriteNoAutoCommit(dest, httpDate.GetValueOrDefault()))
                {
                    await stream.CopyToAsync(destStream);
                    destStream.Commit();
                }
            }

            return dest;
        }


        private static long? TryGetPhotoId(Uri u)
        {
            if (u == null) return null;
            if ((u.IsHostedOn("akamaihd.net") || u.IsHostedOn("fbcdn.net")) && u.AbsolutePath.StartsWith("/safe_image.php")) u = u.GetParameter("url").AsUri();
            if (u.IsHostedOn("facebook.com") && u.AbsolutePath.StartsWith("/l.php")) u = u.GetParameter("u").AsUri();
            if ((u.IsHostedOn("fbcdn.net") || u.IsHostedOn("akamaihd.net")) && (u.AbsolutePath.StartsWith("/v/") || u.AbsolutePath.StartsWith("/hprofile") || u.AbsolutePath.StartsWith("/hphotos") || u.AbsolutePath.Contains("50x50/")))
            {
                return long.Parse(u.AbsolutePath.SplitFast('/').First(x => x.Contains('_')).CaptureBetween("_", "_"));
            }

            if (u.IsHostedOn("facebook.com"))
            {
                if (u.GetPathComponent(1) == "photos")
                {
                    var k = u.GetPathComponent(3);
                    if (k != null) return long.Parse(k);
                }
                if (u.GetPathComponent(0) == "photo.php")
                {
                    return long.Parse(u.GetParameter("fbid"));
                }
            }

            return null;
        }
    }
}
