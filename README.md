# Shaman.Scraping
A library for scraping websites and reading/writing WARC files.

## Reading a CDX/WARC file
```csharp
var items = WarcItem.ReadIndex("path/to/index.cdx");
Stream firstResponseBody = items[0].OpenStream();
```

## WebsiteScraper
Generic implementation of a scraper.
Configurable with `ShouldCrawl`, `Parallelism`, `Cookies`, `CollectAdditionalLinks`.
```csharp
using(var scraper = new WebsiteScraper())
{
    scraper.ShouldScrape = (url, prereq) =>
    {
        if (prereq) return true;
        if (
            url.Host == scraper.FirstAddedUrl.Host && 
            url.Path.StartsWith("/example") &&
            url.HasNoQueryParameters()
            ) return true;

        return false;
    }
}
```

## RedditScraper
Scrapes a subreddit.

## FacebookScraper
Scrapes the content of a Facebook page or a group.

## WikiScraper
Scraper optimized for MediaWiki sites.

## Command line arguments
When run as a console app (as opposed to a library), the following parameters are supported:
* `--make-cdx [path-to-folder]`: Generates index.cdx
* `--website-cookies <cookies>`: Cookies to use
* `--facebook-page <name>`: Scrapes a facebook page